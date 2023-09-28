local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")

local item_dir = os.getenv("item_dir")
local item_names = os.getenv("item_name")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_items = {}
local discovered_outlinks = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

normalize_url = function(url)
  local candidate_current = url
  while true do
    local temp = string.lower(urlparse.unescape(candidate_current))
    if temp == candidate_current then
      break
    end
    candidate_current = temp
  end
  return candidate_current
end

local urls = {}
for url in string.gmatch(item_names, "([^\n]+)") do
  local normalized_url = normalize_url(string.match(url, "^url:(.+)$"))
  local _, slash_count = string.gsub(normalized_url, "/", "")
  if slash_count == 2 then
    normalized_url = normalized_url .. "/"
  end
  urls[normalized_url] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print('discovered', item)
    target[item] = true
    return true
  end
  return false
end

set_item = function(url)
  candidate = normalize_url(url)
  if candidate ~= item_value and urls[candidate] then
    item_type = "url"
    item_value = candidate
    item_name = item_type .. ":" .. item_value
    abortgrab = false
    tries = 0
    retry_url = false
    is_initial_url = true
    print("Archiving item " .. item_name)
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  if item_name == url then
    return true
  end

  if parenturl and parenturl == "version" then
    discover_item(discovered_items, "url:" .. percent_encode_url(url))
  end

  return false
end

queue_all_versions = function(url)
  print("Queuing all for", url)

  local sites = {
    {
      ["perso.wanadoo"]="skip",
      ["perso.orange"]="queue",
      ["pagesperso-orange"]="queue"
    },
    {
      ["monsite.wanadoo"]="queue",
      ["monsite.orange"]="queue",
      ["monsite-orange"]="queue"
    },
    {
      ["pro.wanadoo"]="skip",
      ["pro.orange"]="queue",
      ["pros.orange"]="queue",
      ["pagespro-orange"]="queue",
      ["mairie.pagespro-orange"]="queue",
      ["assoc.pagespro-orange"]="queue",
      ["ecole.pagespro-orange"]="queue"
    }
  }

  if string.match(url, "^https?://[^/]*woopic%.com/") then
    return allowed(url, "version")
  end

  local function queue_all(sub, site, rest)
    if not sub
      or not site
      or not rest
      or (
        sub == "pagespro-orange"
        and (
          site == "mairie"
          or site == "assoc"
          or site == "ecole"
        )
      ) then
      return nil
    end
    local any_match = false
    for _, orange_sites in pairs(sites) do
      local found = false
      for orange_site, _ in pairs(orange_sites) do
        if orange_site == sub then
          found = true
          any_match = true
          break
        end
      end
      if found then
        for orange_site, directive in pairs(orange_sites) do
          if directive == "queue" then
            for _, protocol in pairs({"http", "https"}) do
              allowed(protocol .. "://" .. site .. "." .. orange_site .. ".fr/" .. rest, "version")
              allowed(protocol .. "://" .. orange_site .. ".fr/" .. site .. "/" .. rest, "version")
              allowed(protocol .. "://" .. site .. "." .. orange_site .. ".fr/", "version")
              allowed(protocol .. "://" .. orange_site .. ".fr/" .. site .. "/", "version")
            end
          end
        end
        break
      end
    end
    return any_match
  end

  local sub, site, rest = string.match(url, "^https?://([^/]+)%.fr/([0-9a-zA-Z%-_%.]+)/?(.-)$")
  local match1 = queue_all(sub, site, rest)
  site, sub, rest = string.match(url, "^https?://([0-9a-zA-Z%-_%.]+)%.([^/]+)%.fr/(.-)$")
  local match2 = queue_all(sub, site, rest)
  if not match1 and not match2 then
    discovered_outlinks[url] = true
  else
    allowed(url, "version")
  end
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]

  queue_all_versions(url)

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)  
  downloaded[url] = true

  --queue_all_versions(url)

  return {}
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 301
    and http_stat["statcode"] ~= 302 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if status_code == 200 then
    queue_all_versions(url["url"])
  end
  
  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  local factor = 2
  if status_code ~= 200 then
    factor = 6
  end
  os.execute("sleep " .. tostring(factor*concurrency))

  if status_code == 301 or status_code == 302 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    print("Found redirect to " .. newloc .. ".")
    allowed(newloc, "version")
    if string.match(url["url"], "^https?://(.+)$") == string.match(newloc, "^https?://(.+)$") then
      return wget.actions.EXIT
    end
    local url_sub1, url_site1, url_rest1 = string.match(url["url"], "^https?://([^/]+)%.fr/([0-9a-zA-Z%-_%.]+)/?(.-)$")
    local newloc_site1, newloc_sub1, newloc_rest1 = string.match(newloc, "^https?://([0-9a-zA-Z%-_%.]+)%.([^/]+)%.fr/(.-)$")
    if url_sub1 == newloc_sub1
      and url_site1 == newloc_site1
      and url_rest1 == newloc_rest1 then
      return wget.actions.EXIT
    end
    local url_site2, url_sub2, url_rest2 = string.match(url["url"], "^https?://([0-9a-zA-Z%-_%.]+)%.([^/]+)%.fr/(.-)$")
    local newloc_sub2, newloc_site2, newloc_rest2 = string.match(newloc, "^https?://([^/]+)%.fr/([0-9a-zA-Z%-_%.]+)/?(.-)$")
    if url_sub2 == newloc_sub2
      and url_site2 == newloc_site2
      and url_rest2 == newloc_rest2 then
      return wget.actions.EXIT
    end
    --[[if not string.match(newloc, "/r/Oerreur_404$")
      and not string.match(newloc, "/error404%.html$") then]]
      queue_all_versions(newloc)
      queue_all_versions(url["url"])
    --end
    return wget.actions.EXIT
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = tries - 1
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    downloaded[url["url"]] = true
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["urls-eew3ydz89258xhs1"] = discovered_outlinks,
    ["pagespersoorange-90zcnxoidh7iweq3"] = discovered_items
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end



#!/usr/bin/env tarantool

local clock = require("clock")
local fiber = require("fiber")
local log = require("log")
local net = require("net.box")
local popen = require("popen")

local function getUrl(nodeId)
    return "localhost:" .. tostring(3300 + nodeId)
end

local function queryAll(nodeCount)
    local channel = fiber.channel(nodeCount)
    for i = 1, nodeCount do
        local url = getUrl(i)
        local conn = net.connect(url)

        fiber.create(function()
            local ok, sums = pcall(conn.call, conn, "getSums", nil, {
                timeout = 60
            })

            local err
            if not ok then
                err = sums
                sums = nil
            end

            channel:put({ sums = sums, err = err })
        end)
    end

    local sums = { }
    for _ = 1, nodeCount do
        local res = channel:get()
        if res.sums == nil then
            error(res.err)
        end

        for stateName, sum in pairs(res.sums) do
            sums[stateName] = (sums[stateName] or 0) + sum
        end
    end

    return sums
end

local function run()
    local nodeCount = os.getenv("CUBE_NODE_COUNT") or 8

    log.info("Starting a cluster join...")
    local started = tonumber(clock.time64() / 1000000) / 1000.0

    for i = 1, nodeCount do
        fiber.create(function()
            local ph, err = popen.shell(
                ("CUBE_NODE_ID=%d CUBE_NODE_COUNT=%d ./storage.lua"):format(i, nodeCount), "r"
            )

            if ph == nil then
                error(err)
            end

            log.info(ph:read():rstrip())
        end)

        fiber.sleep(0.1)
    end

    fiber.sleep(1)

    local sums = queryAll(nodeCount)

    local res = {}
    for stateName, sum in pairs(sums) do
        table.insert(res, { state = stateName, sum = sum })
    end
    table.sort(res, function(a, b)
        if a.sum ~= b.sum then
            return a.sum > b.sum
        end

        return a.state < b.state
    end)

    local ended = tonumber(clock.time64() / 1000000) / 1000.0

    fiber.sleep(1)

    log.info("Donations rating:")
    for _, item in ipairs(res) do
        log.info("%s: $%.2f", item.state, item.sum)
    end

    log.info("Execution wall-clock time: %.3f sec", ended - started)
end

run()

os.exit()

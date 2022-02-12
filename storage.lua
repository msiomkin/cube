#!/usr/bin/env tarantool

local csv = require("csv")
local digest = require("digest")
local fio = require("fio")
local log = require("log")

local function isMy(rowId, nodeId, nodeCount)
    return rowId % nodeCount + 1 == nodeId
end

local donorStates
local stateSums
local stateNameToId
local function handleDonors(donorFileName, nodeId, nodeCount)
    local file, fileErr = fio.open(donorFileName, { "O_RDONLY" })
    if file == nil then
        error(fileErr)
    end

    local ok, err = xpcall(function()
        local readCount = 0
        local procCount = 0
        local statesCount = 0
        for i, row in csv.iterate(file, { delimiter = "," }) do
            if i > 1 then
                local donorId = row[1]
                local donorHash = digest.crc32(donorId)

                if isMy(donorHash, nodeId, nodeCount) then
                    local stateName = row[3]
                    assert(stateName ~= nil,
                        ("State name is not specified, line: %d"):format(i))

                    local stateId = stateNameToId[stateName]
                    if stateId == nil then
                        statesCount = statesCount + 1
                        stateId = statesCount
                        stateSums[stateId] = 0
                        stateNameToId[stateName] = stateId
                    end

                    donorStates[donorId] = stateId

                    procCount = procCount + 1
                end

                if i % 1000000 == 0 then
                    log.info("Node %d: Rows read: %d, processed: %d",
                        nodeId, i, procCount)
                end
            end

            readCount = i
        end
        log.info("Node %d: Totally rows read: %d, processed: %d",
            nodeId, readCount, procCount)
    end, debug.traceback)

    file:close()

    if not ok then
        error(err)
    end
end

local function handleDonations(donationFileName, nodeId, nodeCount)
    local file, fileErr = fio.open(donationFileName, { "O_RDONLY" })
    if file == nil then
        error(fileErr)
    end

    local ok, err = xpcall(function()
        local readCount = 0
        local procCount = 0
        for i, row in csv.iterate(file, { delimiter = "," }) do
            if i > 1 then
                local donorId = row[3]
                local donorHash = digest.crc32(donorId)

                if isMy(donorHash, nodeId, nodeCount) then
                    local sum = tonumber(row[5]) or 0

                    local stateId = donorStates[donorId]
                    assert(stateId ~= nil,
                        ("Donor ID not found: %s"):format(donorIdStr))

                    if stateId then
                        stateSums[stateId] = stateSums[stateId] + sum
                    end

                    procCount = procCount + 1
                end
            end

            if i % 1000000 == 0 then
                log.info("Node %d: Rows read: %d, processed: %d",
                    nodeId, i, procCount)
            end

            readCount = i
        end
        log.info("Node %d: Totally rows read: %d, processed: %d",
            nodeId, readCount, procCount)
    end, debug.traceback)

    file:close()

    if not ok then
        error(err)
    end
end

local appCfg
local function getSums()
    donorStates = {}
    stateSums = {}
    stateNameToId = {}

    log.info("Node %d: Calculating sums", appCfg.nodeId)

    log.info("Node %d: Handling donors file...", appCfg.nodeId)
    handleDonors(appCfg.donorsFileName, appCfg.nodeId, appCfg.nodeCount)
    log.info("Node %d: Handling donations file...", appCfg.nodeId)
    handleDonations(appCfg.donationsFileName, appCfg.nodeId, appCfg.nodeCount)

    local res = {}
    for name, id in pairs(stateNameToId) do
        res[name] = stateSums[id]
    end

    log.info("Node %d: Result: ", appCfg.nodeId)
    log.info(res)

    return res
end

local function init()
    local nodeId = tonumber(os.getenv("CUBE_NODE_ID"))
    assert(nodeId, "CUBE_NODE_ID environment variable is not set")

    local nodeCount = tonumber(os.getenv("CUBE_NODE_COUNT"))
    assert(nodeCount, "CUBE_NODE_COUNT environment variable is not set")

    appCfg = {
        nodeId = nodeId,
        nodeCount = nodeCount,
        donorsFileName = "../Donors.csv",
        donationsFileName = "../Donations.csv"
    }

    local port = 3300 + nodeId

    local workDir = ".tmp" .. tostring(nodeId)
    fio.mkdir(workDir)

    box.cfg{
        listen = port,
        checkpoint_interval = 0,
        wal_mode = "none",
        work_dir = workDir
    }

    box.schema.user.grant("guest", "read,write,execute", "universe",
        nil, { if_not_exists = true })

    box.schema.func.create("getSums", { if_not_exists = true })
    box.schema.role.grant("public", "execute", "function", "getSums",
        { if_not_exists = true })

    rawset(_G, "getSums", getSums)
end

init()

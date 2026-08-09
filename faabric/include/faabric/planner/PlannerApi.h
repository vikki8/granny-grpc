#pragma once

namespace faabric::planner {
enum PlannerCalls
{
    NoPlanerCall = 0,
    // Util
    Ping = 1,
    // Host-membership calls
    GetAvailableHosts = 2,
    RegisterHost = 3,
    RemoveHost = 4,
    // Scheduling calls
    SetMessageResult = 8,
    GetMessageResult = 9,
    GetBatchResults = 10,
    GetSchedulingDecision = 11,
    GetNumMigrations = 12,
    CallBatch = 13,
    PreloadSchedulingDecision = 14,
    SetGrpcEndpoint = 15, //COMP70073
    GetGrpcEndpoint = 16,      //COMP70073
    SetGrpcMigrationBlob = 17, //COMP70073 
    PopGrpcMigrationBlob = 18, //COMP70073 
};
}

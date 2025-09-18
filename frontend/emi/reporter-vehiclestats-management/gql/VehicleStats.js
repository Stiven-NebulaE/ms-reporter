import { gql } from 'apollo-boost';

// Query to get fleet statistics
export const ReporterGetFleetStatistics = gql`
    query ReporterGetFleetStatistics {
        ReporterGetFleetStatistics {
            totalVehicles
            vehiclesByType {
                SUV
                PickUp
                Sedan
                Hatchback
                Coupe
            }
            vehiclesByDecade {
                decade1980s
                decade1990s
                decade2000s
                decade2010s
                decade2020s
            }
            vehiclesBySpeedClass {
                Lento
                Normal
                Rapido
            }
            hpStats {
                min
                max
                sum
                count
                avg
            }
            lastUpdated
        }
    }
`;

// Subscription for real-time fleet statistics updates
export const FleetStatisticsUpdatedSubscription = gql`
    subscription FleetStatisticsUpdated {
        ReporterFleetStatisticsUpdated {
            totalVehicles
            vehiclesByType {
                SUV
                PickUp
                Sedan
                Hatchback
                Coupe
            }
            vehiclesByDecade {
                decade1980s
                decade1990s
                decade2000s
                decade2010s
                decade2020s
            }
            vehiclesBySpeedClass {
                Lento
                Normal
                Rapido
            }
            hpStats {
                min
                max
                sum
                count
                avg
            }
            lastUpdated
        }
    }
`;

// Legacy queries (keeping for compatibility)
export const ReporterVehicleStatsListing = (variables) => ({
    query: gql`
            query ReporterVehicleStatsListing($filterInput:ReporterVehicleStatsFilterInput ,$paginationInput:ReporterVehicleStatsPaginationInput,$sortInput:ReporterVehicleStatsSortInput){
                ReporterVehicleStatsListing(filterInput:$filterInput,paginationInput:$paginationInput,sortInput:$sortInput){
                    listing{
                       id,name,active,
                    },
                    queryTotalResultCount
                }
            }`,
    variables,
    fetchPolicy: 'network-only',
})

export const ReporterVehicleStats = (variables) => ({
    query: gql`
            query ReporterVehicleStats($id: ID!, $organizationId: String!){
                ReporterVehicleStats(id:$id, organizationId:$organizationId){
                    id,name,description,active,organizationId,
                    metadata{ createdBy, createdAt, updatedBy, updatedAt }
                }
            }`,
    variables,
    fetchPolicy: 'network-only',
})

export const ReporterCreateVehicleStats = (variables) => ({
    mutation: gql`
            mutation  ReporterCreateVehicleStats($input: ReporterVehicleStatsInput!){
                ReporterCreateVehicleStats(input: $input){
                    id,name,description,active,organizationId,
                    metadata{ createdBy, createdAt, updatedBy, updatedAt }
                }
            }`,
    variables
})

export const ReporterDeleteVehicleStats = (variables) => ({
    mutation: gql`
            mutation ReporterVehicleStatsListing($ids: [ID]!){
                ReporterDeleteVehicleStatss(ids: $ids){
                    code,message
                }
            }`,
    variables
})

export const ReporterUpdateVehicleStats = (variables) => ({
    mutation: gql`
            mutation  ReporterUpdateVehicleStats($id: ID!,$input: ReporterVehicleStatsInput!, $merge: Boolean!){
                ReporterUpdateVehicleStats(id:$id, input: $input, merge:$merge ){
                    id,organizationId,name,description,active
                }
            }`,
    variables
})

export const onReporterVehicleStatsModified = (variables) => ([
    gql`subscription onReporterVehicleStatsModified($id:ID!){
            ReporterVehicleStatsModified(id:$id){    
                id,organizationId,name,description,active,
                metadata{ createdBy, createdAt, updatedBy, updatedAt }
            }
    }`,
    { variables }
])
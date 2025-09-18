import React, { useState, useEffect } from 'react';
import { useSelector } from 'react-redux';
import withReducer from 'app/store/withReducer';
import { FuseLoading } from '@fuse';
import { useQuery, useSubscription } from '@apollo/react-hooks';
import { Box } from '@material-ui/core';
import { ReporterGetFleetStatistics, FleetStatisticsUpdatedSubscription } from '../gql/VehicleStats';
import VehicleStatssHeader from './VehicleStatssHeader';
import VehicleStatssTable from './VehicleStatssTable';
import reducer from '../store/reducers';

function VehicleStatss() {
    const user = useSelector(({ auth }) => auth.user);
    const [fleetStats, setFleetStats] = useState(null);

    // GraphQL query
    const { data: queryData, refetch: refetchStats } = useQuery(ReporterGetFleetStatistics, {
        variables: {
            organizationId: user.selectedOrganization && user.selectedOrganization.id
        },
        fetchPolicy: 'network-only'
    });

    // GraphQL subscription for real-time fleet statistics updates
    const { data: subscriptionData } = useSubscription(FleetStatisticsUpdatedSubscription);

    // Update stats when GraphQL query resolves
    useEffect(() => {
        if (queryData && queryData.ReporterGetFleetStatistics) {
            setFleetStats(queryData.ReporterGetFleetStatistics);
        }
    }, [queryData]);

    // Handle subscription data for real-time updates
    useEffect(() => {
        if (subscriptionData && subscriptionData.ReporterFleetStatisticsUpdated) {
            setFleetStats(subscriptionData.ReporterFleetStatisticsUpdated);
        }
    }, [subscriptionData]);

    if (!user.selectedOrganization) {
        return <FuseLoading />;
    }

    return (
        <Box>
            <VehicleStatssHeader 
                fleetStats={fleetStats}
            />
            <VehicleStatssTable 
                fleetStats={fleetStats}
            />
        </Box>
    );
}

export default withReducer('VehicleStatsManagement', reducer)(VehicleStatss);

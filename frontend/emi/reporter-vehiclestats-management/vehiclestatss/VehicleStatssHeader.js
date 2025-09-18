import React from 'react';
import { Typography, Box, Chip } from '@material-ui/core';

function VehicleStatssHeader({ fleetStats }) {
    const totalVehicles = fleetStats && fleetStats.totalVehicles || 0;
    const lastUpdated = fleetStats && fleetStats.lastUpdated;

    return (
        <Box 
            p={3} 
            bgcolor="primary.main" 
            color="primary.contrastText"
            borderRadius={1}
            mb={2}
        >
            <Box display="flex" alignItems="center" justifyContent="space-between" width="100%">
                <Box>
                    <Typography variant="h4" component="h1" gutterBottom>
                        DASHBOARD DE ANÁLISIS DE FLOTA
                    </Typography>
                    <Box display="flex" alignItems="center" gap={2}>
                        <Typography variant="body1">
                            Total Vehículos: 
                        </Typography>
                        <Chip 
                            label={totalVehicles.toLocaleString()} 
                            color="secondary"
                            variant="outlined"
                            className="ml-1"
                        />
                        {lastUpdated && (
                            <>
                                <Typography variant="body1" className="ml-2">
                                    | Última actualización: 
                                </Typography>
                                <Typography variant="body2" className="ml-1">
                                    {new Date(lastUpdated).toLocaleString()}
                                </Typography>
                            </>
                        )}
                    </Box>
                </Box>
            </Box>
        </Box>
    );
}

export default VehicleStatssHeader;

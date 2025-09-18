import React from 'react';
import { 
    Paper, 
    Typography, 
    Box, 
    Grid, 
    Card, 
    CardContent,
    LinearProgress,
    Chip
} from '@material-ui/core';

function VehicleStatssTable({ fleetStats }) {
    if (!fleetStats) {
        return (
            <Paper elevation={1} p={3}>
                <Typography variant="h6" align="center">
                    Cargando estadísticas...
                </Typography>
            </Paper>
        );
    }

    const { 
        totalVehicles, 
        vehiclesByType, 
        vehiclesByDecade, 
        vehiclesBySpeedClass, 
        hpStats 
    } = fleetStats;

    // Calculate percentages
    const getPercentage = (value) => {
        return totalVehicles > 0 ? ((value / totalVehicles) * 100).toFixed(1) : 0;
    };

    return (
        <Box p={2}>
            <Grid container spacing={3}>
                {/* Vehicles by Type */}
                <Grid item xs={12} md={4}>
                    <Card>
                        <CardContent>
                            <Typography variant="h6" gutterBottom>
                                Vehículos por Tipo
                            </Typography>
                            {Object.entries(vehiclesByType).map(([type, count]) => (
                                <Box key={type} mb={1}>
                                    <Box display="flex" justifyContent="space-between" alignItems="center">
                                        <Typography variant="body2">{type}:</Typography>
                                        <Typography variant="body2">
                                            {count.toLocaleString()} ({getPercentage(count)}%)
                                        </Typography>
                                    </Box>
                                    <LinearProgress 
                                        variant="determinate" 
                                        value={getPercentage(count)} 
                                        style={{ marginTop: 4 }}
                                    />
                                </Box>
                            ))}
                        </CardContent>
                    </Card>
                </Grid>

                {/* Vehicles by Decade */}
                <Grid item xs={12} md={4}>
                    <Card>
                        <CardContent>
                            <Typography variant="h6" gutterBottom>
                                Vehículos por Década
                            </Typography>
                            {Object.entries(vehiclesByDecade).map(([decade, count]) => {
                                const decadeLabel = decade.replace('decade', '').replace('s', 's');
                                return (
                                    <Box key={decade} mb={1}>
                                        <Box display="flex" justifyContent="space-between" alignItems="center">
                                            <Typography variant="body2">{decadeLabel}:</Typography>
                                            <Typography variant="body2">
                                                {count.toLocaleString()} ({getPercentage(count)}%)
                                            </Typography>
                                        </Box>
                                        <LinearProgress 
                                            variant="determinate" 
                                            value={getPercentage(count)} 
                                            style={{ marginTop: 4 }}
                                        />
                                    </Box>
                                );
                            })}
                        </CardContent>
                    </Card>
                </Grid>

                {/* HP Statistics */}
                <Grid item xs={12} md={4}>
                    <Card>
                        <CardContent>
                            <Typography variant="h6" gutterBottom>
                                Potencia (HP)
                            </Typography>
                            <Box mb={2}>
                                <Typography variant="body2">
                                    Mínimo: <Chip label={hpStats.min} size="small" />
                                </Typography>
                            </Box>
                            <Box mb={2}>
                                <Typography variant="body2">
                                    Máximo: <Chip label={hpStats.max} size="small" />
                                </Typography>
                            </Box>
                            <Box mb={2}>
                                <Typography variant="body2">
                                    Promedio: <Chip label={hpStats.avg.toFixed(1)} size="small" />
                                </Typography>
                            </Box>
                        </CardContent>
                    </Card>
                </Grid>

                {/* Speed Classification */}
                <Grid item xs={12}>
                    <Card>
                        <CardContent>
                            <Typography variant="h6" gutterBottom>
                                Clasificación por Velocidad
                            </Typography>
                            <Grid container spacing={2}>
                                {Object.entries(vehiclesBySpeedClass).map(([speedClass, count]) => (
                                    <Grid item xs={12} sm={4} key={speedClass}>
                                        <Box textAlign="center">
                                            <Typography variant="h4" color="primary">
                                                {count.toLocaleString()}
                                            </Typography>
                                            <Typography variant="body2" color="textSecondary">
                                                {speedClass} ({getPercentage(count)}%)
                                            </Typography>
                                            <LinearProgress 
                                                variant="determinate" 
                                                value={getPercentage(count)} 
                                                style={{ marginTop: 8 }}
                                            />
                                        </Box>
                                    </Grid>
                                ))}
                            </Grid>
                        </CardContent>
                    </Card>
                </Grid>
            </Grid>
        </Box>
    );
}

export default VehicleStatssTable;

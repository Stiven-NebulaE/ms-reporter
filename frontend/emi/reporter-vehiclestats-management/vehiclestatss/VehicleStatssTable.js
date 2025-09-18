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
        return totalVehicles > 0 ? ((value / totalVehicles) * 100) : 0;
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
                                            {count.toLocaleString()} ({getPercentage(count).toFixed(1)}%)
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
                                                {count.toLocaleString()} ({getPercentage(count).toFixed(1)}%)
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
                                <Box display="flex" alignItems="center" gap={1}>
                                    <Typography variant="body2">Mínimo:</Typography>
                                    <Chip label={hpStats.min !== null ? hpStats.min : 'N/A'} size="small" />
                                </Box>
                            </Box>
                            <Box mb={2}>
                                <Box display="flex" alignItems="center" gap={1}>
                                    <Typography variant="body2">Máximo:</Typography>
                                    <Chip label={hpStats.max !== null ? hpStats.max : 'N/A'} size="small" />
                                </Box>
                            </Box>
                            <Box mb={2}>
                                <Box display="flex" alignItems="center" gap={1}>
                                    <Typography variant="body2">Suma:</Typography>
                                    <Chip label={hpStats.sum !== null ? hpStats.sum.toLocaleString() : 'N/A'} size="small" />
                                </Box>
                            </Box>
                            <Box mb={2}>
                                <Box display="flex" alignItems="center" gap={1}>
                                    <Typography variant="body2">Promedio:</Typography>
                                    <Chip label={hpStats.avg !== null ? hpStats.avg.toFixed(1) : 'N/A'} size="small" />
                                </Box>
                            </Box>
                            <Box mb={2}>
                                <Box display="flex" alignItems="center" gap={1}>
                                    <Typography variant="body2">Total vehículos:</Typography>
                                    <Chip label={hpStats.count} size="small" />
                                </Box>
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
                                                {speedClass} ({getPercentage(count).toFixed(1)}%)
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

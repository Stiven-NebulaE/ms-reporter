import React from 'react';
import { Redirect } from 'react-router-dom';
import i18n from './i18n'

const auth = ["VEHICLESTATS_READ"];

export const MicroFrontendConfig = {
    settings: {
        layout: {}
    },
    auth,
    routes: [
        { 
            path: '/vehiclestats-mng/vehiclestatss/:vehicleStatsId/:vehicleStatsHandle?',
            component: React.lazy(() => import('./vehiclestats/VehicleStats'))
        },
        {
            path: '/vehiclestats-mng/vehiclestatss',
            component: React.lazy(() => import('./vehiclestatss/VehicleStatss'))
        },
        {
            path: '/vehiclestats-mng',
            component: () => <Redirect to="/vehiclestats-mng/vehiclestatss" />
        }
    ],
    navigationConfig: [
        {
            'id': 'settings',
            'type': 'collapse',
            'icon': 'settings',
            'priority': 100,
            children: [{
                'id': 'reporter-vehiclestats-management',
                'type': 'item',
                'icon': 'business',
                'url': '/vehiclestats-mng',
                'priority': 2000,
                auth
            }]
        }
    ],
    i18nLocales: i18n.locales
};


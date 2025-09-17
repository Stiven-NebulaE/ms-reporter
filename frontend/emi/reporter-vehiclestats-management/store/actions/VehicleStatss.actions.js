import { defer } from 'rxjs';
import { mergeMap, map } from 'rxjs/operators';

import graphqlService from '../../../../services/graphqlService';
import { ReporterVehicleStatsListing, ReporterDeleteVehicleStats } from '../../gql/VehicleStats';

export const SET_VEHICLESTATSS = '[VEHICLESTATS_MNG] SET VEHICLESTATSS';
export const SET_VEHICLESTATSS_PAGE = '[VEHICLESTATS_MNG] SET VEHICLESTATSS PAGE';
export const SET_VEHICLESTATSS_ROWS_PER_PAGE = '[VEHICLESTATS_MNG] SET VEHICLESTATSS ROWS PER PAGE';
export const SET_VEHICLESTATSS_ORDER = '[VEHICLESTATS_MNG] SET VEHICLESTATSS ORDER';
export const SET_VEHICLESTATSS_FILTERS_ORGANIZATION_ID = '[VEHICLESTATS_MNG] SET VEHICLESTATSS FILTERS ORGANIZATION_ID';
export const SET_VEHICLESTATSS_FILTERS_NAME = '[VEHICLESTATS_MNG] SET VEHICLESTATSS FILTERS NAME';
export const SET_VEHICLESTATSS_FILTERS_ACTIVE = '[VEHICLESTATS_MNG] SET VEHICLESTATSS FILTERS ACTIVE';

/**
 * Common function to generate the arguments for the ReporterVehicleStatsListing query based on the user input
 * @param {Object} queryParams 
 */
function getListingQueryArguments({ filters: { name, organizationId, active }, order, page, rowsPerPage }) {
    const args = {
        "filterInput": { organizationId },
        "paginationInput": { "page": page, "count": rowsPerPage, "queryTotalResultCount": (page === 0) },
        "sortInput": order.id ? { "field": order.id, "asc": order.direction === "asc" } : undefined
    };
    if (name.trim().length > 0) {
        args.filterInput.name = name;
    }
    if (active !== null) {
        args.filterInput.active = active;
    }
    return args;
}

/**
 * Queries the VehicleStats Listing based on selected filters, page and order
 * @param {{ filters, order, page, rowsPerPage }} queryParams
 */
export function getVehicleStatss({ filters, order, page, rowsPerPage }) {
    const args = getListingQueryArguments({ filters, order, page, rowsPerPage });    
    return (dispatch) => graphqlService.client.query(ReporterVehicleStatsListing(args)).then(result => {
        return dispatch({
            type: SET_VEHICLESTATSS,
            payload: result.data.ReporterVehicleStatsListing
        });
    })
}

/**
 * Executes the mutation to remove the selected rows
 * @param {*} selectedForRemovalIds 
 * @param {*} param1 
 */
export function removeVehicleStatss(selectedForRemovalIds, { filters, order, page, rowsPerPage }) {
    const deleteArgs = { ids: selectedForRemovalIds };
    const listingArgs = getListingQueryArguments({ filters, order, page, rowsPerPage });
    return (dispatch) => defer(() => graphqlService.client.mutate(ReporterDeleteVehicleStats(deleteArgs))).pipe(
        mergeMap(() => defer(() => graphqlService.client.query(ReporterVehicleStatsListing(listingArgs)))),
        map((result) =>
            dispatch({
                type: SET_VEHICLESTATSS,
                payload: result.data.ReporterVehicleStatsListing
            })
        )
    ).toPromise();
}

/**
 * Set the listing page
 * @param {int} page 
 */
export function setVehicleStatssPage(page) {
    return {
        type: SET_VEHICLESTATSS_PAGE,
        page
    }
}

/**
 * Set the number of rows to see per page
 * @param {*} rowsPerPage 
 */
export function setVehicleStatssRowsPerPage(rowsPerPage) {
    return {
        type: SET_VEHICLESTATSS_ROWS_PER_PAGE,
        rowsPerPage
    }
}

/**
 * Set the table-column order
 * @param {*} order 
 */
export function setVehicleStatssOrder(order) {
    return {
        type: SET_VEHICLESTATSS_ORDER,
        order
    }
}

/**
 * Set the name filter
 * @param {string} name 
 */
export function setVehicleStatssFilterName(name) {    
    return {
        type: SET_VEHICLESTATSS_FILTERS_NAME,
        name
    }
}

/**
 * Set the filter active flag on/off/both
 * @param {boolean} active 
 */
export function setVehicleStatssFilterActive(active) {
    return {
        type: SET_VEHICLESTATSS_FILTERS_ACTIVE,
        active
    }
}

/**
 * set the organizationId filter
 * @param {string} organizationId 
 */
export function setVehicleStatssFilterOrganizationId(organizationId) {    
    return {
        type: SET_VEHICLESTATSS_FILTERS_ORGANIZATION_ID,
        organizationId
    }
}




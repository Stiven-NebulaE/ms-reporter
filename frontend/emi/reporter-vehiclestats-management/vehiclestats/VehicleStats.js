/* React core */
import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
/* UI core */
import { Button, Tab, Tabs, TextField, Icon, Typography, Switch, FormControlLabel } from '@material-ui/core';
import { FuseAnimate, FusePageCarded, FuseLoading } from '@fuse';
import { useForm } from '@fuse/hooks';
/* GraphQL Client hooks */
import { useSubscription, useLazyQuery, useMutation } from "@apollo/react-hooks";
/* Redux */
import { useDispatch, useSelector } from 'react-redux';
import withReducer from 'app/store/withReducer';
import * as AppActions from 'app/store/actions';
import * as Actions from '../store/actions';
import reducer from '../store/reducers';
/* Tools */
import _ from '@lodash';
import { Formik } from 'formik';
import * as Yup from "yup";
import { MDText } from 'i18n-react';
import i18n from "../i18n";
/* Support pages */
import Error404Page from 'app/main/pages/Error404Page';
import Error500Page from 'app/main/pages/Error500Page';
/* GQL queries/mutation to use */
import {
    onReporterVehicleStatsModified,
    ReporterVehicleStats,
    ReporterCreateVehicleStats,
    ReporterUpdateVehicleStats
} from "../gql/VehicleStats";
import Metadata from './tabs/Metadata';
import { BasicInfo, basicInfoFormValidationsGenerator } from './tabs/BasicInfo';


/**
 * Default Aggregate data when creating 
 */
const defaultData = {
    name: '',
    description: '',
    active: true,
};

function VehicleStats(props) {
    //Redux dispatcher
    const dispatch = useDispatch();

    // current logged user
    const loggedUser = useSelector(({ auth }) => auth.user);

    // VehicleStats STATE and CRUD ops
    const [vehicleStats, setVehicleStats] = useState();
    const gqlVehicleStats = ReporterVehicleStats({ id: props.match.params.vehicleStatsId });
    const [readVehicleStats, readVehicleStatsResult] = useLazyQuery(gqlVehicleStats.query, { fetchPolicy: gqlVehicleStats.fetchPolicy })
    const [createVehicleStats, createVehicleStatsResult] = useMutation(ReporterCreateVehicleStats({}).mutation);
    const [updateVehicleStats, updateVehicleStatsResult] = useMutation(ReporterUpdateVehicleStats({}).mutation);
    const onVehicleStatsModifiedResult = useSubscription(...onReporterVehicleStatsModified({ id: props.match.params.vehicleStatsId }));

    //UI controls states
    const [tabValue, setTabValue] = useState(0);
    const { form, handleChange: formHandleChange, setForm } = useForm(null);
    const [errors, setErrors] = useState([]);

    //Translation services
    let T = new MDText(i18n.get(loggedUser.locale));

    /*
    *  ====== USE_EFFECT SECTION ========
    */

    /*
        Prepares the FORM:
            - if is NEW then use default data
            - if is old VehicleStats then loads the data
        Reads (from the server) a VehicleStats when:
            - having a valid props.match.params (aka ID)
            - having or changing the selected Organization ID
    */
    useEffect(() => {
        function updateVehicleStatsState() {
            const params = props.match.params;
            const { vehicleStatsId } = params;
            if (vehicleStatsId !== 'new') {
                if (loggedUser.selectedOrganization && loggedUser.selectedOrganization.id !== "") {
                    readVehicleStats({ variables: { organizationId: loggedUser.selectedOrganization.id, id: vehicleStatsId } });
                }
            } else if (loggedUser.selectedOrganization && loggedUser.selectedOrganization.id) {
                setVehicleStats({ ...defaultData, organizationId: loggedUser.selectedOrganization.id })
                dispatch(Actions.setVehicleStatssPage(0));
            }
        }
        updateVehicleStatsState();
    }, [dispatch, props.match.params, loggedUser.selectedOrganization]);


    //Refresh VehicleStats state when the lazy query (READ) resolves
    useEffect(() => {
        if (readVehicleStatsResult.data)
            setVehicleStats(readVehicleStatsResult.data.ReporterVehicleStats)
    }, [readVehicleStatsResult])
    //Refresh VehicleStats state when the CREATE mutation resolves
    useEffect(() => {
        if (createVehicleStatsResult.data && createVehicleStatsResult.data.ReporterCreateVehicleStats) {
            setVehicleStats(createVehicleStatsResult.data.ReporterCreateVehicleStats)
            props.history.push('/vehiclestats-mng/vehiclestatss/' + createVehicleStatsResult.data.ReporterCreateVehicleStats.id + '/');
            dispatch(AppActions.showMessage({ message: T.translate("vehiclestats.create_success"), variant: 'success' }));
        }

    }, [createVehicleStatsResult])
    //Refresh VehicleStats state when the UPDATE mutation resolves
    useEffect(() => {
        if (updateVehicleStatsResult.data) {
            setVehicleStats(updateVehicleStatsResult.data.ReporterUpdateVehicleStats);
        }
    }, [updateVehicleStatsResult])
    //Refresh VehicleStats state when GQL subscription notifies a change
    useEffect(() => {
        if (onVehicleStatsModifiedResult.data) {
            setForm(onVehicleStatsModifiedResult.data.ReporterVehicleStatsModified);
            dispatch(AppActions.showMessage({ message: T.translate("vehiclestats.update_success"), variant: 'success' }));
        }
    }, [onVehicleStatsModifiedResult.data]);


    // Keep the sync between the VehicleStats state and the form state
    useEffect(() => {
        if ((vehicleStats && !form) || (vehicleStats && form && vehicleStats.id !== form.id)) {
            setForm(vehicleStats);
        }
    }, [form, vehicleStats, setForm]);

    // DISPLAYS floating message for CRUD errors
    useEffect(() => {
        const error = createVehicleStatsResult.error || updateVehicleStatsResult.error;
        if (error) {
            const { graphQLErrors, networkError, message } = error;
            const errMessage = networkError
                ? JSON.stringify(networkError)
                : graphQLErrors.length === 0
                    ? message
                    : graphQLErrors[0].message.name
            dispatch(AppActions.showMessage({
                message: errMessage,
                variant: 'error'
            }));
        }
    }, [createVehicleStatsResult.error, updateVehicleStatsResult.error])

    /*
    *  ====== FORM HANDLERS, VALIDATORS AND LOGIC ========
    */

    /**
     * Handles Tab changes
     * @param {*} event 
     * @param {*} tabValue 
     */
    function handleChangeTab(event, tabValue) {
        setTabValue(tabValue);
    }

    /**
     * Evaluates if the logged user has enought permissions to WRITE (Create/Update/Delete) data
     */
    function canWrite() {
        return loggedUser.role.includes('VEHICLESTATS_WRITE');
    }

    /**
     * Evals if the Save button can be submitted
     */
    function canBeSubmitted() {
        return (
            canWrite()
            && !updateVehicleStatsResult.loading
            && !createVehicleStatsResult.loading
            && _.isEmpty(errors)
            && !_.isEqual({ ...vehicleStats, metadata: undefined }, { ...form, metadata: undefined })
        );
    }

    /**
     * Handle the Save button action
     */
    function handleSave() {
        const { id } = form;
        if (id === undefined) {
            createVehicleStats({ variables: { input: { ...form, organizationId: loggedUser.selectedOrganization.id } } });
        } else {
            updateVehicleStats({ variables: { id, input: { ...form, id: undefined, __typename: undefined, metadata: undefined }, merge: true } });
        }
    }

    /*
    *  ====== ALTERNATIVE PAGES TO RENDER ========
    */

    // Shows an ERROR page when a really important server response fails
    const gqlError = readVehicleStatsResult.error;
    if (gqlError) {
        const firstErrorMessage = gqlError.graphQLErrors[0].message;
        if (!firstErrorMessage.includes || !firstErrorMessage.includes("Cannot return null")) {
            return (<Error500Page message={T.translate("vehiclestats.internal_server_error")}
                description={gqlError.graphQLErrors.map(e => `@${e.path[0]} => code ${e.message.code}: ${e.message.name}`)} />);
        }
    }

    // Shows the Loading bar if we are waiting for something mandatory
    if (!loggedUser.selectedOrganization || readVehicleStatsResult.loading) {
        return (<FuseLoading />);
    }

    // Shows a NotFound page if the VehicleStats has not been found. (maybe because it belongs to other organization or the id does not exists)
    if (props.match.params.vehicleStatsId !== "new" && !readVehicleStatsResult.data) {
        return (<Error404Page message={T.translate("vehiclestats.not_found")} />);
    }


    /*
    *  ====== FINAL PAGE TO RENDER ========
    */

    return (
        <FusePageCarded
            classes={{
                toolbar: "p-0",
                header: "min-h-72 h-72 sm:h-136 sm:min-h-136"
            }}
            header={
                form && (
                    <div className="flex flex-1 w-full items-center justify-between">

                        <div className="flex flex-col items-start max-w-full">

                            <FuseAnimate animation="transition.slideRightIn" delay={300}>
                                <Typography className="normal-case flex items-center sm:mb-12" component={Link} role="button" to="/vehiclestats-mng/vehiclestatss" color="inherit">
                                    <Icon className="mr-4 text-20">arrow_back</Icon>
                                    {T.translate("vehiclestats.vehiclestatss")}
                                </Typography>
                            </FuseAnimate>

                            <div className="flex items-center max-w-full">
                                <FuseAnimate animation="transition.expandIn" delay={300}>
                                    <Icon className="text-32 mr-0 sm:text-48 mr-12">business</Icon>
                                </FuseAnimate>

                                <div className="flex flex-col min-w-0">
                                    <FuseAnimate animation="transition.slideLeftIn" delay={300}>
                                        <Typography className="text-16 sm:text-20 truncate">
                                            {form.name ? form.name : 'New VehicleStats'}
                                        </Typography>
                                    </FuseAnimate>
                                    <FuseAnimate animation="transition.slideLeftIn" delay={300}>
                                        <Typography variant="caption">{T.translate("vehiclestats.vehiclestats_detail")}</Typography>
                                    </FuseAnimate>
                                </div>
                            </div>
                        </div>
                        <FuseAnimate animation="transition.slideRightIn" delay={300}>
                            <Button
                                className="whitespace-no-wrap"
                                variant="contained"
                                disabled={!canBeSubmitted()}
                                onClick={handleSave}
                            >
                                {T.translate("vehiclestats.save")}
                            </Button>
                        </FuseAnimate>
                    </div>
                )
            }
            contentToolbar={
                <Tabs
                    value={tabValue}
                    onChange={handleChangeTab}
                    indicatorColor="secondary"
                    textColor="secondary"
                    variant="scrollable"
                    scrollButtons="auto"
                    classes={{ root: "w-full h-64" }}
                >
                    <Tab className="h-64 normal-case" label={T.translate("vehiclestats.basic_info")} />

                    {(form && form.metadata) && (<Tab className="h-64 normal-case" label={T.translate("vehiclestats.metadata_tab")} />)}
                </Tabs>
            }
            content={
                form && (
                    <div className="p-16 sm:p-24 max-w-2xl">

                        <Formik
                            initialValues={{ ...form }}
                            enableReinitialize
                            onSubmit={handleSave}
                            validationSchema={Yup.object().shape({
                                ...basicInfoFormValidationsGenerator(T)
                            })}

                        >

                            {(props) => {
                                const {
                                    values,
                                    touched,
                                    errors,
                                    setFieldTouched,
                                    handleChange,
                                    handleSubmit
                                } = props;

                                setErrors(errors);
                                const onChange = (fieldName) => (event) => {
                                    event.persist();
                                    setFieldTouched(fieldName);
                                    handleChange(event);
                                    formHandleChange(event);
                                };

                                return (
                                    <form noValidate onSubmit={handleSubmit}>
                                        {tabValue === 0 && <BasicInfo dataSource={values} {...{ T, onChange, canWrite, errors, touched }} />}
                                        {tabValue === 1 && <Metadata dataSource={values} T={T} />}
                                    </form>
                                );
                            }}
                        </Formik>



                    </div>
                )
            }
            innerScroll
        />
    )
}

export default withReducer('VehicleStatsManagement', reducer)(VehicleStats);

const withFilter = require("graphql-subscriptions").withFilter;
const PubSub = require("graphql-subscriptions").PubSub;
const pubsub = new PubSub();
const { of } = require("rxjs");
const { tap, map, mergeMap, catchError } = require('rxjs/operators');
let broker = require("../../broker/BrokerFactory")();
broker = broker.secondaryBroker ? broker.secondaryBroker : broker;
const RoleValidator = require('../../tools/RoleValidator');
const { sendToBackEndHandler$ } = require('../../tools/GraphqlResponseTools');

const INTERNAL_SERVER_ERROR_CODE = 1;
const PERMISSION_DENIED_ERROR_CODE = 2;
const CONTEXT_NAME = "organization-mng";

const READ_ROLES = ["ORGANIZATION_READ", "ORGANIZATION_INFO_READ"];
const WRITE_ROLES = ["ORGANIZATION_WRITE"];


module.exports = {

    //// QUERY ///////
    Query: {
        OrganizationMngOrganizationListing(root, args, context) {
            return sendToBackEndHandler$(root, args, context, READ_ROLES, 'query', 'Organization', 'OrganizationMngOrganizationListing',10000,CONTEXT_NAME).toPromise();
        },
        OrganizationMngOrganization(root, args, context) {
            return sendToBackEndHandler$(root, args, context, READ_ROLES, 'query', 'Organization', 'OrganizationMngOrganization',10000,CONTEXT_NAME).toPromise();
        },
        OrganizationMngParentOrganization(root , args , context){
            return sendToBackEndHandler$(root, args, context, READ_ROLES, 'query', 'Organization', 'OrganizationMngParentOrganization',2000,CONTEXT_NAME).toPromise();
        }
    },

    //// MUTATIONS ///////
    Mutation: {
        OrganizationMngCreateOrganization(root, args, context) {
            return sendToBackEndHandler$(root, args, context, WRITE_ROLES, 'mutation', 'Organization', 'OrganizationMngCreateOrganization',2000,CONTEXT_NAME).toPromise();
        },
        OrganizationMngUpdateOrganization(root, args, context) {
            return sendToBackEndHandler$(root, args, context, WRITE_ROLES, 'mutation', 'Organization', 'OrganizationMngUpdateOrganization',2000,CONTEXT_NAME).toPromise();
        },
        OrganizationMngDeleteOrganizations(root, args, context) {
            return sendToBackEndHandler$(root, args, context, WRITE_ROLES, 'mutation', 'Organization', 'OrganizationMngDeleteOrganizations',2000,CONTEXT_NAME).toPromise();
        },
    },

    //// SUBSCRIPTIONS ///////
    Subscription: {
        OrganizationMngOrganizationModified: {
            subscribe: withFilter(
                (payload, variables, context, info) => {
                    //Checks the roles of the user, if the user does not have at least one of the required roles, an error will be thrown
                    RoleValidator.checkAndThrowError(
                        context.authToken.realm_access.roles,
                        READ_ROLES,
                        "OrganizationMng",
                        "OrganizationMngOrganizationModified",
                        PERMISSION_DENIED_ERROR_CODE,
                        "Permission denied"
                    );
                    return pubsub.asyncIterator("OrganizationMngOrganizationModified");
                },
                (payload, variables, context, info) => {
                    return payload
                        ? (payload.OrganizationMngOrganizationModified.id === variables.id) || (variables.id === "ANY")
                        : false;
                }
            )
        }
    }
};


//// SUBSCRIPTIONS SOURCES ////

const eventDescriptors = [
    {
        backendEventName: "OrganizationMngOrganizationModified",
        gqlSubscriptionName: "OrganizationMngOrganizationModified",
        dataExtractor: evt => evt.data, // OPTIONAL, only use if needed
        onError: (error, descriptor) =>
            console.log(`Error processing ${descriptor.backendEventName}`), // OPTIONAL, only use if needed
        onEvent: (evt, descriptor) =>
            console.log(`Event of type  ${descriptor.backendEventName} arrived`) // OPTIONAL, only use if needed
    }
];

/**
 * Connects every backend event to the right GQL subscription
 */
eventDescriptors.forEach(descriptor => {
    broker.getMaterializedViewsUpdates$([descriptor.backendEventName]).subscribe(
        evt => {
            if (descriptor.onEvent) {
                descriptor.onEvent(evt, descriptor);
            }
            const payload = {};
            payload[descriptor.gqlSubscriptionName] = descriptor.dataExtractor
                ? descriptor.dataExtractor(evt)
                : evt.data;
            pubsub.publish(descriptor.gqlSubscriptionName, payload);
        },

        error => {
            if (descriptor.onError) {
                descriptor.onError(error, descriptor);
            }
            console.error(`Error listening ${descriptor.gqlSubscriptionName}`, error);
        },

        () => console.log(`${descriptor.gqlSubscriptionName} listener STOPED.`)
    );
});

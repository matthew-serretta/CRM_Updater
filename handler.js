'use strict';
const { Client } = require("pg");
const fs = require("fs");
const axios = require('axios').default;

module.exports.query = async event => {

    await updateInHubSpot(await getUsers(`${process.env.CUSTOMER_DB}`), `${process.env.CUSTOMER_DB}`)

    return {
        statusCode: 200,
        body: JSON.stringify(
            {
                message: "Update has run successfully",
                input: event,
            },
            null,
            2
        ),
    };
};

async function getUsers(database) {

    console.log(`querying ${database} database`);

    try {
        const client = await getClient(database);
        const queryResult = await runQuery(client);

        console.log(`query returned ${queryResult.rowCount} result(s)`);
        return queryResult.rows

    } catch (err) {
        console.error(err);
    }
}

async function getClient(database) {
    const client = new Client({
        user: `${process.env.DB_USER}`,
        host: `${process.env.DB_HOST}`,
        database: database,
        password: `${process.env.DB_PASSWORD}`,
        ssl: {
            rejectUnauthorized: false,
            ca: [fs.readFileSync('./rds-cert.pem')]
        },
        port: 5432,
    });
    await client.connect()
    return client;
}

async function runQuery(client) {
    const query = `
        select
            users.*,
            signupReasons."name" as signupReason,
            preferredContactMethods."name" as preferredContactMethod,
            loc."searchString" as fullLocation,
            loc."name" as city,
            parentLoc."name" as state,
            country."name" as country
        from
            "Users" users
        left join
            "SignupReasons" signupReasons on users."signupReasonId" = signupReasons.id
        left join
            "PreferredContactMethods" preferredContactMethods on users."preferredContactMethodId" = preferredContactMethods.id
        left join
            wywm_locations."location" loc on users."currentLocationId" = loc.id
        left join
            wywm_locations."location" parentLoc on loc.parentid = parentLoc.id
        left join
            wywm_locations."country" country on loc."countryid" = country.id
        where
            users."updatedAt" > NOW() - INTERVAL '15 minutes';
        `
    return new Promise((resolve, reject) => {
        client.query(query, (err, res) => {
            if (err) {
                client.end();
                return reject(err);
            }
            client.end();
            return resolve(res);
        });
    });
}

async function updateInHubSpot(users, database) {

    if (!users || users.length <= 0) {
        console.log(`no users to update in ${database} or passed invalid user array`)
        return
    }

    const HubSpotAPILimit = 1000
    if (users.length > HubSpotAPILimit) {
        for (var i = 0; i < users.length; i += HubSpotAPILimit) {
            await updateInHubSpot(users.slice(i, i + HubSpotAPILimit), database);
        }
        return
    }

    const url = `https://api.hubapi.com/contacts/v1/contact/batch/?hapikey=${process.env.HAPIKEY}`
    const body = generateProps(users)

    let userIds = []
    for (var user of users) {
        userIds.push(user.id)
    }
    console.log(`sending post request for ${database} users: ${userIds}`)

    try {
        const result = await axios.post(url, body);
        console.log(`post request status: ${result.status} ${result.statusText}`);
    } catch (err) {
        console.error("post request failed");
    }
}

function generateProps(users) {
    var props = []

    for (var user of users) {
        var userProps = {
            "email": user.username,
            "properties": [
                {
                    "property": "firstname",
                    "value": user.firstName
                },
                {
                    "property": "lastname",
                    "value": user.lastName
                },
                {
                    "property": "jobtitle",
                    "value": user.currentJobTitle
                },
                {
                    "property": "preferred_contact_method",
                    "value": user.preferredcontactmethod
                },
                {
                    "property": "phone",
                    "value": user.contactPhone
                },
                {
                    "property": "date_of_birth",
                    "value": convertToHubSpotDateFormat(user.birthdate)
                },
                {
                    "property": "current_location",
                    "value": user.fulllocation
                },
                {
                    "property": "city",
                    "value": user.city
                },
                {
                    "property": "state",
                    "value": user.state
                },
                {
                    "property": "country",
                    "value": user.country
                },
                {
                    "property": "linkedin",
                    "value": user.linkedinProfileUrl
                },
                {
                    "property": "id",
                    "value": user.id
                },
                {
                    "property": "further_sign_up_information",
                    "value": user.signupreason
                },
                {
                    "property": "comfort_zone",
                    "value": user.responseToExpOutsideComfortZone
                },
                {
                    "property": "created_at",
                    "value": convertToHubSpotDateFormat(user.createdAt)
                },
                {
                    "property": "last_updated",
                    "value": convertToHubSpotDateFormat(user.updatedAt)
                },
                {
                    "property": "big_five_complete_date_time",
                    "value": convertToHubSpotDateFormat(user.doneBigFiveAssessmentAt)
                },
                {
                    "property": "aptitude_complete_date_time",
                    "value": convertToHubSpotDateFormat(user.doneGeneralAptitudeTestAt)
                },
                {
                    "property": "learning_style_complete_date",
                    "value": convertToHubSpotDateFormat(user.doneLearningStyleAssessmentAt)
                },
                {
                    "property": "disc_complete_date_time",
                    "value": convertToHubSpotDateFormat(user.doneDISCAt)
                },
                {
                    "property": "disc_type",
                    "value": user.discType
                },
            ]
        }
        props.push(userProps)
    }

    return props
}

function convertToHubSpotDateFormat(date) {

    if (!date) {
        return ""
    }

    var newDate = new Date(date)
    newDate.setUTCHours(0, 0, 0, 0)
    return newDate.getTime()
}

const AWS = require('aws-sdk');
const axios = require('node_modules/axios/lib/axios.js');
const dynamodb = new AWS.DynamoDB({
    region: 'us-east-1',
    apiVersion: '2012-08-10'
});
// const cisp = new AWS.CognitoIdentityServiceProvider({ apiVersion: '2016-04-18' });
// const stepfunctions = new AWS.StepFunctions();

exports.handler = (event, context, callback) => {

    console.log('event = ', event);
    if (event.Records && event.Records[0].eventName === 'INSERT') {
        // const accessToken = event.accessToken;
        // const candidateID = event.candidateID;
        // const criteriaSet = decodeURIComponent(event.criteriaSet);

        console.log('Order details: ', event);
        console.log('eventName', event.Records[0].eventName);
        console.log('object', event.Records[0].dynamodb);
        console.log('price', event.Records[0].dynamodb.NewImage.maxPrice.N);
        console.log('minProductRating', event.Records[0].dynamodb.NewImage.minProductRating.N);
        console.log('bestOfferType1', event.Records[0].dynamodb.NewImage.bestOfferType.S);
        console.log('productID', event.Records[0].dynamodb.NewImage.productID.S);
        console.log('quantity', event.Records[0].dynamodb.NewImage.quantity.N);

        // const cispParams = {
        //     "AccessToken": accessToken
        // };
        // cisp.getUser(cispParams, (err, result) => {
        // if (err) {
        //     console.log(err);
        //     callback(err);
        // } else {

        const productIDFromUser = event.Records[0].dynamodb.NewImage.productID.S;
        const priceFromUser = event.Records[0].dynamodb.NewImage.maxPrice.N;
        const quantityDemanded = event.Records[0].dynamodb.NewImage.quantity.N;
        console.log('productIDFromUser - ', productIDFromUser);
        const params = {
            TableName: 'OfferTable',
            IndexName: "productID-price-index",
            KeyConditionExpression: "productID = :a", // and price < :p",
            ExpressionAttributeValues: {
                ":a": {
                    S: productIDFromUser
                },
                // ":p": {
                //     N: priceFromUser
                // }
            },
            ProjectionExpression: "companyID, offerID, available, price, productID",
            ScanIndexForward: false
        };

        dynamodb.query(params, function (err, data) {
            if (err) {
                console.log(err);
                callback(err);
            } else {
                console.log('data OfferTable query result', data);
                let minPrice = 1000000;
                let companyID;
                let offerID;
                if (event.Records[0].dynamodb.NewImage.bestOfferType.S === 'CHEAPEST') {
                    data.Items.map((item) => {
                        if (parseFloat(item.price.N) < minPrice && item.available.N > quantityDemanded) {
                            minPrice = parseFloat(item.price.N);
                            companyID = item.companyID;
                            offerID = item.offerID;
                        }
                    });
                    console.log('bestOffer2', minPrice, companyID, offerID);
                } else if (event.Records[0].dynamodb.NewImage.bestOfferType.S === 'OPTIMAL') {
                    data.Items.map((item) => {
                        if (parseFloat(item.price.N) < minPrice && item.available.N > quantityDemanded) {
                            minPrice = parseFloat(item.price.N);
                            companyID = item.companyID;
                            offerID = item.offerID;
                        }
                    });
                    const newText = 'bestOffer 3' + ' - ' + minPrice + ' - ' + companyID + ' - ' + offerID;
                    console.log(newText);
                    sendToInvoke(event, newText);
                }
                console.log('offer data = ', data);
                console.log('price offer ', data.Items[0].price.N, )
                console.log('available offer ', data.Items[0].available.N, )
                console.log('productID offer ', data.Items[0].productID.S, )
            }
        });

        // }
        // });
    }
};


const sendToInvoke = async (event, newText) => {

    AWS.config.update({
        region: 'us-east-1',
        credentials: new AWS.Credentials({
            accessKeyId: process.env.AccessKeyId,
            secretAccessKey: process.env.SecretAccessKey,
        })
    });
    const result = await invokeAppSync({
        text1: newText,
        text2: 'LD15'
    });

    console.log(result);
    return result.data;
};

const invokeAppSync = async ({
    text1,
    text2
}) => {
    let req = new AWS.HttpRequest('https://abc.appsync-api.us-east-1.amazonaws.com/graphql', 'us-east-1');
    req.method = 'POST';
    req.headers.host = 'abc.appsync-api.us-east-1.amazonaws.com';
    req.headers['Content-Type'] = 'multipart/form-data';
    req.body = JSON.stringify({
        "query": "mutation ($input: CreateProductInput!) { createProduct(input: $input){  id, name, modelNo, specificationURL, imageURL, lastTenRatingAverage } }",
        "variables": {
            "input": {
                "name": text1,
                "modelNo": text2,
                specificationURL: "String2",
                imageURL: "String2",
                lastTenRatingAverage: 2.9
            }
        }
    });
    let signer = new AWS.Signers.V4(req, 'appsync', true);
    signer.addAuthorization(AWS.config.credentials, AWS.util.date.getDate());
    const result = await axios({
        method: 'post',
        url: 'https://abc.appsync-api.us-east-1.amazonaws.com/graphql',
        data: req.body,
        headers: req.headers
    });
    return result;
};
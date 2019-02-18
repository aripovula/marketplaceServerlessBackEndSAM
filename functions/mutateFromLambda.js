const AWS = require('aws-sdk');
const axios = require('node_modules/axios/lib/axios.js');
const uuid = require('node_modules/uuid/v4.js');
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

        if (!event.Records || !event.Records[0] || !event.Records[0].dynamodb || !event.Records[0].dynamodb.NewImage) {
            console.log("data_not_valid");
            callback(null, "data_not_valid");
        }

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

        const theOrder = event.Records[0].dynamodb.NewImage;
        const productIDFromUser = event.Records[0].dynamodb.NewImage.productID.S;
        const priceFromUserStr = event.Records[0].dynamodb.NewImage.maxPrice.N;
        const priceFromUser = parseFloat(priceFromUserStr);
        const quantityDemandedStr = event.Records[0].dynamodb.NewImage.quantity.N;
        const quantityDemanded = parseInt(quantityDemandedStr);
        const orderID = event.Records[0].dynamodb.NewImage.orderID.S;
        console.log('productIDFromUser - ', productIDFromUser);

        // read all Offers for ordered product
        const params = {
            TableName: 'OfferTable',
            IndexName: "productID-price-index",
            KeyConditionExpression: "productID = :a and price < :maxPrice",
            ExpressionAttributeValues: {
                ":a": {
                    S: productIDFromUser
                },
                ":maxPrice": {
                    N: priceFromUserStr
                }
            },
            ProjectionExpression: "companyID, offerID, available, price, productID",
            ScanIndexForward: false
        };

        dynamodb.query(params, function (err, data) {
            if (err) {
                console.log(err);
                callback(null, "offer_query_failed");
            } else {
                // if query is succesful find the best offer based on BestOfferType
                console.log('data OfferTable query result', data);
                let theOffer;
                let minPrice = 1000000;
                let companyID;
                let offerID;
                if (event.Records[0].dynamodb.NewImage.bestOfferType.S === 'CHEAPEST') {
                    data.Items.map((item) => {
                        console.log('before condition-', item.price.N, minPrice, item.available.N, quantityDemanded);
                        if (parseFloat(item.price.N) < minPrice && parseInt(item.available.N) > quantityDemanded) {
                            theOffer = item;
                            console.log("in map offer is ", theOffer);
                            minPrice = parseFloat(item.price.N);
                            companyID = item.companyID;
                            offerID = item.offerID;
                        }
                    });
                    console.log("after map offer is ", theOffer);
                    // const newText = 'bestOffer 2' + ' - ' + minPrice + ' - ' + companyID.S + ' - ' + orderID;
                    // console.log('b4 invoke, newText =', newText);
                    if (theOrder && theOffer) {
                        sendToInvoke(event, theOrder, theOffer);
                    } else {
                        console.log("offer_or_order_not_valid");
                        callback(null, "offer_or_order_not_valid");
                    }


                } else if (event.Records[0].dynamodb.NewImage.bestOfferType.S === 'OPTIMAL') {
                    data.Items.map((item) => {
                        console.log('before condition-', item.price.N, minPrice, item.available.N, quantityDemanded);
                        if (parseFloat(item.price.N) < minPrice && parseInt(item.available.N) > quantityDemanded) {
                            theOffer = item;
                            console.log("in map offer is ", theOffer);
                            minPrice = parseFloat(item.price.N);
                            companyID = item.companyID;
                            offerID = item.offerID;
                        }
                    });
                    console.log("after map offer is ", theOffer);
                    // const newText = 'bestOffer 3' + ' - ' + minPrice + ' - ' + companyID.S + ' - ' + orderID;
                    // console.log('b4 invoke, newText =', newText);
                    if (theOrder && theOffer) {
                        sendToInvoke(event, theOrder, theOffer);
                    } else {
                        console.log("offer_or_order_not_valid");
                        callback(null, "offer_or_order_not_valid");
                    }
                }
                console.log('offer data = ', data);
                console.log('price offer ', data.Items[0].price.N, )
                console.log('available offer ', data.Items[0].available.N, )
                console.log('productID offer ', data.Items[0].productID.S, )
            }
        });

        // }
        // });
    } else {
        callback(null, "only_insert_is_processed");
    }
};


const sendToInvoke = async (event, theOrder, theOffer) => {
    // console.log('in sendToInvoke, newText -', newText);
    AWS.config.update({
        region: 'us-east-1',
        credentials: new AWS.Credentials({
            accessKeyId: process.env.AccessKeyId,
            secretAccessKey: process.env.SecretAccessKey,
        })
    });
    const result = await invokeAppSyncNewDeal(theOrder, theOffer);
    const result2 = await invokeAppSyncUpdateOffer(theOrder, theOffer);
    const result3 = await invokeAppSyncUpdateOrder(theOrder, theOffer);

    console.log('result = ', result);
    // console.log('result.erors = ', result.data.erors[0]);
    return result.data;
};

const invokeAppSyncNewDeal = async (theOrder, theOffer) => {
    let req = new AWS.HttpRequest('https://abc.appsync-api.us-east-1.amazonaws.com/graphql', 'us-east-1');
    req.method = 'POST';
    req.headers.host = 'abc.appsync-api.us-east-1.amazonaws.com';
    req.headers['Content-Type'] = 'multipart/form-data';
    req.body = JSON.stringify({
        "query": "mutation ($input: CreateDealInput!) { createDeal(input: $input){  productID, dealID, orderID, buyerID, producerID, dealTime, dealPrice, dealQuantity, productRatingByBuyer, blockchainBlockID, dealStatus, blockchainBlockStatus } }",
        "variables": {
            "input": {
                productID: theOrder.productID.S,
                dealID: uuid(),
                orderID: theOrder.orderID.S,
                buyerID: theOrder.companyID.S,
                producerID: theOffer.companyID.S,
                dealTime: new Date() * 1,
                dealPrice: theOffer.price.N,
                dealQuantity: theOrder.quantity.N,
                productRatingByBuyer: null,
                blockchainBlockID: "A",
                dealStatus: "DEAL_MADE",
                blockchainBlockStatus: "INITIATED"
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

const invokeAppSyncUpdateOffer = async (theOrder, theOffer) => {
    let req = new AWS.HttpRequest('https://abc.appsync-api.us-east-1.amazonaws.com/graphql', 'us-east-1');
    req.method = 'POST';
    req.headers.host = 'abc.appsync-api.us-east-1.amazonaws.com';
    req.headers['Content-Type'] = 'multipart/form-data';
    req.body = JSON.stringify({
        "query": "mutation ($input: UpdateOfferInput!) { updateOffer(input: $input){  companyID, offerID, productID, price, available } }",
        "variables": {
            "input": {
                companyID: theOffer.companyID.S,
                offerID: theOffer.offerID.S,
                available: (parseInt(theOffer.available.N) - parseInt(theOrder.quantity.N))
            }
        }
    });
    console.log("req.body--", req.body);
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

const invokeAppSyncUpdateOrder = async (theOrder, theOffer) => {
    let req = new AWS.HttpRequest('https://abc.appsync-api.us-east-1.amazonaws.com/graphql', 'us-east-1');
    req.method = 'POST';
    req.headers.host = 'abc.appsync-api.us-east-1.amazonaws.com';
    req.headers['Content-Type'] = 'multipart/form-data';
    req.body = JSON.stringify({
        "query": "mutation ($input: UpdateOrderInput!) { updateOrder(input: $input){companyID,orderID,productID,product{id,name,modelNo,specificationURL,imageURL,lastTenRatingAverage},status,maxPrice,quantity,bestOfferType,secondBestOfferType,minProductRating,isCashPayment } }",
        "variables": {
            "input": {
                companyID: theOrder.companyID.S,
                orderID: theOrder.orderID.S,
                maxPrice: parseFloat(theOffer.price.N),
                status: "DEAL_MADE"
            }
        }
    });
    console.log("req.body--", req.body);
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
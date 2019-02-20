const AWS = require('aws-sdk');
const axios = require('node_modules/axios/lib/axios.js');
const uuid = require('node_modules/uuid/v4.js');
const dynamodb = new AWS.DynamoDB({
    region: process.env.AppSyncRegion,
    apiVersion: '2012-08-10'
});


exports.handler = (event, context, callback) => {

    console.log('event = ', event);
    if (event.Records && event.Records[0].eventName === 'INSERT') {

        if (!event.Records || !event.Records[0] || !event.Records[0].dynamodb || !event.Records[0].dynamodb.NewImage) {
            console.log("data_not_valid");
            callback(null, "data_not_valid");
        }

        // AWS.config.update({
        //     region: process.env.AppSyncRegion,
        //     credentials: new AWS.Credentials({
        //         accessKeyId: process.env.AccessKeyId,
        //         secretAccessKey: process.env.SecretAccessKey,
        //     })
        // });

        // console.log('Order details: ', event);
        // console.log('eventName', event.Records[0].eventName);
        // console.log('object', event.Records[0].dynamodb);
        // console.log('price', event.Records[0].dynamodb.NewImage.maxPrice.N);
        // console.log('minProductRating', event.Records[0].dynamodb.NewImage.minProductRating.N);
        console.log('bestOfferType1', event.Records[0].dynamodb.NewImage.bestOfferType.S);
        // console.log('productID', event.Records[0].dynamodb.NewImage.productID.S);
        // console.log('quantity', event.Records[0].dynamodb.NewImage.quantity.N);

        const theOrder = event.Records[0].dynamodb.NewImage;
        const bestOfferType = event.Records[0].dynamodb.NewImage.bestOfferType.S;
        const productIDFromUser = event.Records[0].dynamodb.NewImage.productID.S;
        const priceFromUserStr = event.Records[0].dynamodb.NewImage.maxPrice.N;
        const minProductRating = parseFloat(event.Records[0].dynamodb.NewImage.minProductRating.N);
        const priceFromUser = parseFloat(priceFromUserStr);
        const quantityDemandedStr = event.Records[0].dynamodb.NewImage.quantity.N;
        const quantityDemanded = parseInt(quantityDemandedStr);
        const orderID = event.Records[0].dynamodb.NewImage.orderID.S;
        let dealsForRating;
        console.log('productIDFromUser - ', productIDFromUser);


        if (bestOfferType == "OPTIMAL" || bestOfferType == "CUSTOM") {
            if (productIDFromUser) {
                const paramsDeals = {
                    TableName: 'DealTable',
                    KeyConditionExpression: "productID = :a",
                    ExpressionAttributeValues: {
                        ":a": {
                            S: productIDFromUser
                        }
                    },
                    ProjectionExpression: "dealID, producerID, productRatingByBuyer, dealTime",
                };

                dynamodb.query(paramsDeals, function (err, data) {
                    if (err) {
                        console.log(null, 'deals_query_error-' + err);
                        callback(null, "dealsForRating_deals_offer_query_failed");
                    } else if (data.Items && data.Items[0] && data.Items[0].productRatingByBuyer.N) {
                        dealsForRating = JSON.parse(JSON.stringify(data));
                        console.log('dealsForRating - ', dealsForRating);
                    }
                });
            }
        }



        // read all Offers for ordered product
        const paramsOffer = {
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

        dynamodb.query(paramsOffer, function (err, data) {
            if (err) {
                console.log(null, 'query_error-' + err);
                callback(null, "offer_query_failed");
            } else if (data.Items && data.Items[0] && data.Items[0].price.N) {
                // if query is succesful find the best offer based on BestOfferType
                console.log('data OfferTable query result', data);
                let theOffer;
                let minPrice = 1000000;
                let companyID;
                let offerID;
                if (isVolumeAvailable(data, quantityDemanded)) {
                    if (bestOfferType === 'CHEAPEST') {
                        theOffer = findCheapestOffer(data, minPrice, quantityDemanded);
                        if (theOrder && theOffer) {
                            invokeMutationsForGivenType(event, theOrder, theOffer, 'NewDeal');
                        } else {
                            console.log("offer_or_order_not_valid");
                            callback(null, "offer_or_order_not_valid");
                        }

                    } else if (bestOfferType === 'OPTIMAL') {
                        theOffer = findCheapestOfferWithMinRating(callback, data, minPrice, minProductRating, dealsForRating, quantityDemanded);
                        if (theOrder && theOffer) {
                            invokeMutationsForGivenType(event, theOrder, theOffer, 'NewDeal');
                        } else if (!theOffer && theOrder) {
                            theOffer = findCheapestOffer(data, minPrice, quantityDemanded);
                            if (theOrder && theOffer) {
                                invokeMutationsForGivenType(event, theOrder, theOffer, 'NewDeal');
                            } else {
                                console.log("offer_or_order_not_valid");
                                callback(null, "offer_or_order_not_valid");
                            }
                        } else {
                            console.log("offer_or_order_not_valid");
                            callback(null, "offer_or_order_not_valid");
                        }
                    }
                } else {
                    invokeMutationsForGivenType(event, theOrder, theOffer, 'Insufficient');
                }

            } else if (data.Items && data.Items.length === 0) {
                console.log("offer_query_data_not_valid data -", data);
                console.log("offer_query_data_not_valid data.Items", data.Items.length);
                console.log("offer_query_data_not_valid data.Items", data.Items);
                invokeMutationsForGivenType(event, theOrder, null, 'NotOffered');
            } else {
                console.log("offer_query_data_not_valid data -", data);
                if (data.Items) {
                    console.log("offer_query_data_not_valid data.Items", data.Items.length);
                    console.log("offer_query_data_not_valid data.Items", data.Items);
                }
                callback(null, "offer_query_data_not_valid");
            }
        });

        // }
        // });
    } else {
        callback(null, "only_insert_is_processed");
    }
};

const isVolumeAvailable = (data, quantityDemanded) => {
    let isAvailable = false;
    data.Items.map((item) => {
        console.log('before condition-', item.available.N, quantityDemanded);
        if (parseInt(item.available.N) > quantityDemanded) {
            isAvailable = true;
        }
    });
    return isAvailable;
}

const findCheapestOffer = (data, minPrice, quantityDemanded) => {
    let theOffer;
    data.Items.map((item) => {
        console.log('before condition-', item.price.N, minPrice, item.available.N, quantityDemanded);
        if (parseFloat(item.price.N) <= minPrice && parseInt(item.available.N) >= quantityDemanded) {
            theOffer = item;
            console.log("in map offer is ", theOffer);
        }
    });
    // in real life app I would add code that would find cheapest offer with best rating if 
    // there are two offers with the same lowest price. Forr simplicity I skipped this.
    return theOffer;
}

const findCheapestOfferWithMinRating = (callback, data, minPrice, minProductRating, dealsForRating, quantityDemanded) => {
    if (data.Items && dealsForRating.Items) {
        let theOffer;
        data.Items.map((item) => {
            console.log('before condition-', item.price.N, minPrice, item.available.N, quantityDemanded);

            let count = 0,
                total = 0;
            let theRating;
            let allCompanyProductRatings = [];
            dealsForRating.Items.map((deal) => {
                if (deal.producerID == item.companyID) allCompanyProductRatings.push({
                    rating: deal.productRatingByBuyer,
                    time: deal.dealTime
                });
            });
            allCompanyProductRatings.sort((a, b) => b.time.localeCompare(a.time)).map((deal) => {
                count++;
                if (count < 11) {
                    total = total + deal.rating;
                }
            });
            theRating = total / 10;
            console.log('total, rating ', total, theRating);
            if (parseFloat(item.price.N) <= minPrice &&
                parseInt(item.available.N) >= quantityDemanded &&
                theRating >= minProductRating) {
                theOffer = item;
                console.log("in map offer is ", theOffer);
            }
        });
        return theOffer;
    } else {
        console.log('data.Items_or_dealsForRating.Items_not_defined');
        callback(null, 'data.Items_or_dealsForRating.Items_not_defined');
    }
    // in real life app I would add code that would find cheapest offer with best rating if 
    // there are two offers with the same lowest price. Forr simplicity I skipped this.
}

const invokeMutationsForGivenType = async (event, theOrder, theOffer, mutationType) => {
    AWS.config.update({
        region: process.env.AppSyncRegion,
        credentials: new AWS.Credentials({
            accessKeyId: process.env.AccessKeyId,
            secretAccessKey: process.env.SecretAccessKey,
        })
    });

    switch (mutationType) {
        case 'NewDeal':
            await invokeAppSyncWitCustomBody(getQueryTextForNewDeal(theOrder, theOffer));
            await invokeAppSyncWitCustomBody(getQueryTextForUpdateOffer(theOrder, theOffer));
            await invokeAppSyncWitCustomBody(getQueryTextForUpdateOrder(theOrder, theOffer));
            await invokeAppSyncWitCustomBody(getQueryTextForNotifySeller(theOrder, theOffer));
            break;
        case 'Insufficient':
            await invokeAppSyncWitCustomBody(getQueryTextForInsufficientStock(theOrder, theOffer));
            break;
        case 'NotOffered':
            await invokeAppSyncWitCustomBody(getQueryTextForProductNotOffered(theOrder, theOffer));
            break;
    }
    return;
};

const getQueryTextForNewDeal = (theOrder, theOffer) => {
    return JSON.stringify({
        "query": "mutation ($input: CreateDealInput!) { createDeal(input: $input){  productID, dealID, orderID, buyerID, producerID, dealTime, dealPrice, dealQuantity, productRatingByBuyer, blockchainBlockID, dealStatus, blockchainBlockStatus } }",
        "variables": {
            "input": {
                productID: theOrder.productID.S,
                dealID: new Date() * 1, // uuid(),
                orderID: theOrder.orderID.S,
                buyerID: theOrder.companyID.S,
                producerID: theOffer.companyID.S,
                dealTime: new Date() * 1,
                dealPrice: theOffer.price.N,
                dealQuantity: theOrder.quantity.N,
                productRatingByBuyer: null,
                blockchainBlockID: "A",
                dealStatus: "DEAL_MADE",
                blockchainBlockStatus: "INITIATED",
                productRatingByBuyer: Math.floor(Math.random() * (4.8 - 4.4 + 1)) + 4.4
            }
        }
    });
};

const getQueryTextForUpdateOffer = (theOrder, theOffer) => {
    return JSON.stringify({
        "query": "mutation ($input: UpdateOfferInput!) { updateOffer(input: $input){  companyID, offerID, productID, price, available } }",
        "variables": {
            "input": {
                companyID: theOffer.companyID.S,
                offerID: theOffer.offerID.S,
                available: (parseInt(theOffer.available.N) - parseInt(theOrder.quantity.N))
            }
        }
    });
};

const getQueryTextForUpdateOrder = (theOrder, theOffer) => {
    return JSON.stringify({
        "query": "mutation ($input: UpdateOrderInput!) { updateOrder(input: $input){companyID,orderID,productID,product{id,name,modelNo,specificationURL,imageURL,lastTenRatingAverage},status,maxPrice,quantity,bestOfferType,secondBestOfferType,minProductRating,isCashPayment,dealPrice,note } }",
        "variables": {
            "input": {
                companyID: theOrder.companyID.S,
                orderID: theOrder.orderID.S,
                dealPrice: parseFloat(theOffer.price.N),
                status: "DEAL_MADE"
            }
        }
    });
};

const getQueryTextForNotifySeller = (theOrder, theOffer) => {
    return JSON.stringify({
        "query": "mutation ($input: CreateNotificationInput!) { createNotification(input: $input){companyID,notificationID,notificationTextRegular,notificationTextHighlighted } }",
        "variables": {
            "input": {
                companyID: theOrder.companyID.S,
                notificationID: new Date() * 1, // uuid(),
                notificationTextRegular: theOrder.quantity.N + ' were sold at ' + theOffer.price.N,
                notificationTextHighlighted: 'status: confirmed'
            }
        }
    });
};

const getQueryTextForInsufficientStock = (theOrder, theOffer) => {
    return JSON.stringify({
        "query": "mutation ($input: UpdateOrderInput!) { updateOrder(input: $input){companyID,orderID,productID,product{id,name,modelNo,specificationURL,imageURL,lastTenRatingAverage},status,maxPrice,quantity,bestOfferType,secondBestOfferType,minProductRating,isCashPayment,dealPrice,note } }",
        "variables": {
            "input": {
                companyID: theOrder.companyID.S,
                orderID: theOrder.orderID.S,
                note: "qnty not available",
                status: "REJECTED"
            }
        }
    });
};

const getQueryTextForProductNotOffered = (theOrder, theOffer) => {
    return JSON.stringify({
        "query": "mutation ($input: UpdateOrderInput!) { updateOrder(input: $input){companyID,orderID,productID,product{id,name,modelNo,specificationURL,imageURL,lastTenRatingAverage},status,maxPrice,quantity,bestOfferType,secondBestOfferType,minProductRating,isCashPayment,dealPrice,note } }",
        "variables": {
            "input": {
                companyID: theOrder.companyID.S,
                orderID: theOrder.orderID.S,
                note: "product not offered",
                status: "REJECTED"
            }
        }
    });
};

// add new invocations above this line
const invokeAppSyncWitCustomBody = async (theBody) => {
    let req = new AWS.HttpRequest(process.env.AppSyncURL, process.env.AppSyncRegion);
    req.method = 'POST';
    req.headers.host = process.env.AppSyncHost;
    req.headers['Content-Type'] = 'multipart/form-data';
    req.body = theBody;
    let signer = new AWS.Signers.V4(req, 'appsync', true);
    signer.addAuthorization(AWS.config.credentials, AWS.util.date.getDate());
    const result = await axios({
        method: 'post',
        url: process.env.AppSyncURL,
        data: req.body,
        headers: req.headers
    });
    return result;
};

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

        // console.log('Order details: ', event);
        // console.log('eventName', event.Records[0].eventName);
        // console.log('object', event.Records[0].dynamodb);
        // console.log('price', event.Records[0].dynamodb.NewImage.maxPrice.N);
        // console.log('minProductRating', event.Records[0].dynamodb.NewImage.minProductRating.N);
        console.log('bestOfferType1', event.Records[0].dynamodb.NewImage.bestOfferType.S);
        // console.log('productID', event.Records[0].dynamodb.NewImage.productID.S);
        // console.log('quantity', event.Records[0].dynamodb.NewImage.quantity.N);

        queryDataFindBestOfferAndSendBack(event, callback);

    } else {
        callback(null, "only_insert_is_processed");
    }
}

const queryDataFindBestOfferAndSendBack = async (event, callback) => {
    const theOrder = event.Records[0].dynamodb.NewImage;
    const bestOfferType = event.Records[0].dynamodb.NewImage.bestOfferType.S;
    const secondBestOfferType = event.Records[0].dynamodb.NewImage.secondBestOfferType.S;
    const productIDFromUser = event.Records[0].dynamodb.NewImage.productID.S;
    const maxPriceFromUserStr = event.Records[0].dynamodb.NewImage.maxPrice.N;
    const minProductRating = parseFloat(event.Records[0].dynamodb.NewImage.minProductRating.N);
    const maxPriceFromUser = parseFloat(maxPriceFromUserStr);
    const quantityDemandedStr = event.Records[0].dynamodb.NewImage.quantity.N;
    const quantityDemanded = parseInt(quantityDemandedStr);
    const orderID = event.Records[0].dynamodb.NewImage.orderID.S;
    let dealsForRating = await getDealForRating(callback, bestOfferType, productIDFromUser);
    console.log('productIDFromUser - ', productIDFromUser);


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
                N: maxPriceFromUserStr
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
            console.log('data b4 available filter ', data);
            const NewItems = data.Items.filter((item) => parseInt(item.available.N) >= quantityDemanded);
            data = {
                Items: NewItems
            }
            console.log('data after available filter ', data);
            if (data.Items && data.Items.length > 0) {
                if (bestOfferType === 'CHEAPEST') {
                    theOffer = findCheapestOffer(callback, data, dealsForRating, minPrice, quantityDemanded);
                } else if (bestOfferType === 'OPTIMAL') {
                    theOffer = findCheapestOfferWithMinRating(callback, data, minPrice, minProductRating, dealsForRating, quantityDemanded);
                    if (!theOffer) theOffer = trySecondBestOption(callback, data, dealsForRating, minPrice, minProductRating, maxPriceFromUser, quantityDemanded, secondBestOfferType);
                } else if (bestOfferType === 'HIGHESTRATING') {
                    theOffer = findHighestRatedOffer(callback, data, minPrice, minProductRating, maxPriceFromUser, dealsForRating, quantityDemanded);
                } else if (bestOfferType === 'CUSTOM') {
                    theOffer = findBestOfferWithCustomSettings(callback, data, minPrice, minProductRating, maxPriceFromUser, dealsForRating, quantityDemanded);
                    if (!theOffer) theOffer = trySecondBestOption(callback, data, dealsForRating, minPrice, minProductRating, maxPriceFromUser, quantityDemanded, secondBestOfferType);
                }
                if (theOrder && theOffer) {
                    invokeMutationsForGivenType(event, theOrder, theOffer, 'NewDeal');
                } else {
                    console.log("offer_or_order_not_valid");
                    callback(null, "offer_or_order_not_valid");
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
            console.log("offer_query-data_not_valid-data -", data);
            if (data.Items) {
                console.log("offer_query-data_not_valid-data.Items", data.Items.length);
                console.log("offer_query-data_not_valid-data.Items", data.Items);
            }
            callback(null, "offer_query_data_not_valid");
        }
    });

};

const getDealForRating = async (callback, bestOfferType, productIDFromUser) => {
    let dealsForRating;
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
                    console.log('dealsForRating 1- ', dealsForRating);
                }
            });
        }
    }
}

const trySecondBestOption = (callback, data, dealsForRating, minPrice, minProductRating, maxPriceFromUser, quantityDemanded, secondBestOfferType) => {
    if (secondBestOfferType === 'CHEAPEST') {
        return findCheapestOffer(callback, data, dealsForRating, minPrice, quantityDemanded);
    } else if (secondBestOfferType === 'HIGHESTRATING') {
        findHighestRatedOffer(callback, data, minPrice, minProductRating, maxPriceFromUser, dealsForRating, quantityDemanded);
    }
}

const findAllHighestRatedOffers = (data) => {
    let minRating = 0;
    // find lowest rating
    data.Items.map((item) => {
        if (item.lastTenAverageRating >= minRating) {
            minRating = item.lastTenAverageRating;
        }
    });
    // find all at this highest rating
    let highestRatedOffers = [];
    highestRatedOffers.Items.map((item) => {
        if (item.lastTenAverageRating == minRating) {
            highestRatedOffers.push(item);
        }
    });
    return highestRatedOffers;
}

const findAllCheapestOffers = (data) => {
    // find the cheapest price
    let minPrice = 1000000;
    data.Items.map((item) => {
        if (parseFloat(item.price.N) <= minPrice) {
            minPrice = parseFloat(item.price.N);
        }
    });

    // check if two or more offers at same cheapest price exist 
    let cheapestOffers = [];
    data.Items.map((item) => {
        if (parseFloat(item.price.N) === minPrice) {
            cheapestOffers.push(item);
        }
    });
    return cheapestOffers;
}

const findCheapestOffer = (callback, data, dealsForRating, minPrice, quantityDemanded) => {
    let theOffer;
    // find the cheapest price
    let cheapestOffers = findAllCheapestOffers(data);

    // if one - return cheapest one
    if (cheapestOffers.length == 1) {
        theOffer = cheapestOffers[0];
        // if more than one - select cheapest one with highest rating
    } else if (cheapestOffers.length > 1) {
        cheapestOffers = addAverageRatingToOffersByCompany(callback, cheapestOffers, dealsForRating);
        cheapestOffers = findAllHighestRatedOffers(cheapestOffers);
        theOffer = cheapestOffers[0];
    }
    return theOffer;
}

const findCheapestOfferWithMinRating = (callback, data, minPrice, minProductRating, dealsForRating, quantityDemanded) => {
    let theOffer;
    if (data) console.log('b4 condition3 (data)', data);
    if (dealsForRating) console.log('b4 condition3 (dealsForRating)', dealsForRating);
    if (data && data.Items && dealsForRating && dealsForRating.Items) {
        data = addAverageRatingToOffersByCompany(callback, data, dealsForRating);
        if (data) console.log('after addAverageRatingToOffersByCompany - data-', data);
        data = data.Items.filter((item) => item.lastTenAverageRating >= minProductRating);
        theOffer = findCheapestOffer(callback, data, dealsForRating, minPrice, quantityDemanded);
    }
    return theOffer;
}

const findHighestRatedOffer = (callback, data, minPrice, minProductRating, maxPriceFromUser, dealsForRating, quantityDemanded) => {
    let theOffer;
    data = data.Items.filter((item) => parseFloat(item.price.N) <= maxPriceFromUser);
    data = addAverageRatingToOffersByCompany(callback, data, dealsForRating);
    // find the highest rating among offers
    let highestRatedOffers = findAllHighestRatedOffers(data);

    // if one - return cheapest one
    if (highestRatedOffers.length == 1) {
        theOffer = highestRatedOffers[0];
        // if more than one - select cheapest one
    } else if (highestRatedOffers.length > 1) {
        highestRatedOffers = findAllCheapestOffers(highestRatedOffers);
        theOffer = highestRatedOffers[0];
    }
    return theOffer;
}

const findBestOfferWithCustomSettings = (callback, data, minPrice, minProductRating, maxPriceFromUser, dealsForRating, quantityDemanded) => {
    let theOffer;
    if (data && data.Items && dealsForRating && dealsForRating.Items) {
        data = data.Items.filter((item) => parseFloat(item.price.N) <= maxPriceFromUser);
        data = addAverageRatingToOffersByCompany(callback, data, dealsForRating);
        data = data.Items.filter((item) => item.lastTenAverageRating >= minProductRating);
        theOffer = findCheapestOffer(callback, data, dealsForRating, minPrice, quantityDemanded);
    }
    return theOffer;
}

const addAverageRatingToOffersByCompany = (callback, offersForProduct, dealsForRating) => {
    if (offersForProduct && offersForProduct.Items && dealsForRating && dealsForRating.Items) {
        for (let x = 0; x < offersForProduct.length; x++) {
            for (let y = 0; y < dealsForRating.length; y++) {
                if (offersForProduct[x].companyID === dealsForRating[y].producerID) {
                    offersForProduct[x]["lastTenAverageRating"] = getLastTenAverageRating(offersForProduct[x].companyID, dealsForRating);
                }
            }
        }
    }
    return offersForProduct;
}

const getLastTenAverageRating = (coID, dealsForRating) => {
    console.log('getLastTenAverageRating -coID, dealsForRating -', coID, dealsForRating);
    let theRating = 0; // 0 is an initial rating for a product that does not have a rating.
    // sort out deals for this coID
    if (coID && dealsForRating && dealsForRating.Items) {
        let count = 0,
            total = 0;
        let allCompanyProductRatings = [];
        dealsForRating.Items.map((deal) => {
            if (deal.producerID.S == coID) {
                allCompanyProductRatings.push({
                    rating: parseFloat(deal.productRatingByBuyer.N),
                    time: parseInt(deal.dealTime.N)
                });
            }
        });
        console.log('allCompanyProductRatings sorted', allCompanyProductRatings.sort((a, b) => b.time - a.time));
        // sort result by time
        // calculate average rating

        allCompanyProductRatings.sort((a, b) => b.time - a.time).map((deal) => {
            count++;
            console.log('count++', count);
            if (count < 11) {
                total = total + deal.rating;
                console.log('in count<11 total -', total, deal.rating.N);
            }
        });
        console.log('total, rating ', total, theRating);
        theRating = total / count;
    }
    return theRating;
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
                productRatingByBuyer: ((Math.random() * (4.8 - 4.4)) + 4.4).toFixed(2)
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

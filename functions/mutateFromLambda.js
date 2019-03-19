const AWS = require('aws-sdk');
const axios = require('node_modules/axios/lib/axios.js');
const uuid = require('node_modules/uuid/v4.js');
const sha256 = require('js-sha256');
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

        console.log('Order details: ', event);
        console.log('eventName', event.Records[0].eventName);
        console.log('object', event.Records[0].dynamodb);
        console.log('price', event.Records[0].dynamodb.NewImage.maxPrice.N);
        console.log('minProductRating', event.Records[0].dynamodb.NewImage.minProductRating.N);
        console.log('bestOfferType1', event.Records[0].dynamodb.NewImage.bestOfferType.S);
        console.log('productID', event.Records[0].dynamodb.NewImage.productID.S);
        console.log('quantity', event.Records[0].dynamodb.NewImage.quantity.N);

        const productIDFromUser = event.Records[0].dynamodb.NewImage.productID.S;
        let dealsForRating;

        const paramsDeals = {
            TableName: 'DealTable',
            KeyConditionExpression: "productID = :a",
            ExpressionAttributeValues: {
                ":a": {
                    S: productIDFromUser
                }
            },
            // ProjectionExpression: "productID, dealID, producerID, productRatingByBuyer, dealTime, blockchainBlockOfDeal",
        };

        dynamodb.query(paramsDeals, function (err, data) {
            if (err) {
                console.log(null, 'deals_query_error-' + err);
                callback(null, "dealsForRating_deals_offer_query_failed");
            } else {
                dealsForRating = JSON.parse(JSON.stringify(data));
                console.log('dealsForRating 1- ', dealsForRating);
                queryDataFindBestOfferAndSendBack(event, callback, dealsForRating, productIDFromUser);
            }
        });
    } else {
        callback(null, "only_insert_is_processed");
    }
    console.log('no_offers_found');
    //callback(null, "no_offers_found");
}

const queryDataFindBestOfferAndSendBack = (event, callback, dealsForRating, productIDFromUser) => {
    const theOrder = event.Records[0].dynamodb.NewImage;
    const bestOfferType = event.Records[0].dynamodb.NewImage.bestOfferType.S;
    const secondBestOfferType = event.Records[0].dynamodb.NewImage.secondBestOfferType.S;
    // const productIDFromUser = event.Records[0].dynamodb.NewImage.productID.S;
    const maxPriceFromUserStr = event.Records[0].dynamodb.NewImage.maxPrice.N;
    const minProductRating = parseFloat(event.Records[0].dynamodb.NewImage.minProductRating.N);
    const maxPriceFromUser = parseFloat(maxPriceFromUserStr);
    const quantityDemandedStr = event.Records[0].dynamodb.NewImage.quantity.N;
    const quantityDemanded = parseInt(quantityDemandedStr);
    const orderID = event.Records[0].dynamodb.NewImage.orderID.S;
    // let dealsForRating = await getDealForRating(callback, bestOfferType, productIDFromUser);
    // if (dealsForRating) console.log('dealsForRating', dealsForRating);
    // console.log('productIDFromUser - ', productIDFromUser);


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
            // const NewItems = ;
            data = {
                Items: data.Items.filter((item) => parseInt(item.available.N) >= quantityDemanded)
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
                    invokeMutationsForGivenType(event, theOrder, theOffer, 'NewDeal', dealsForRating);
                } else {
                    console.log("offer_or_order_not_valid");
                    invokeMutationsForGivenType(event, theOrder, null, 'NoneFound', dealsForRating);
                    // callback(null, "offer_or_order_not_valid");
                }

            } else {
                invokeMutationsForGivenType(event, theOrder, theOffer, 'Insufficient', dealsForRating);
            }
        } else if (data.Items && data.Items.length === 0) {
            console.log("offer_query_data_not_valid data -", data);
            console.log("offer_query_data_not_valid data.Items", data.Items.length);
            console.log("offer_query_data_not_valid data.Items", data.Items);
            invokeMutationsForGivenType(event, theOrder, null, 'NotOffered', dealsForRating);
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

const trySecondBestOption = (callback, data, dealsForRating, minPrice, minProductRating, maxPriceFromUser, quantityDemanded, secondBestOfferType) => {
    console.log('in trySecondBestOption', secondBestOfferType);
    if (secondBestOfferType === 'CHEAPEST') {
        return findCheapestOffer(callback, data, dealsForRating, minPrice, quantityDemanded);
    } else if (secondBestOfferType === 'HIGHESTRATING') {
        findHighestRatedOffer(callback, data, minPrice, minProductRating, maxPriceFromUser, dealsForRating, quantityDemanded);
    }
}

const findAllHighestRatedOffers = (data) => {
    let highestRatedOffers = [];
    if (data && data.Items) {
        let minRating = 0;
        // find lowest rating
        data.Items.map((item) => {
            if (item.lastTenAverageRating >= minRating) {
                minRating = item.lastTenAverageRating;
            }
        });
        console.log('RATING APPLIED TO BEST OFFER', minRating);
        // find all at this highest rating
        data.Items.map((item) => {
            if (item.lastTenAverageRating == minRating) {
                highestRatedOffers.push(item);
            }
        });
    }
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
    let cheapestHighestRated;
    console.log("cheapestOffers 1-", cheapestOffers);
    // if one - return cheapest one
    if (cheapestOffers.length == 1) {
        theOffer = cheapestOffers[0];
        // if more than one - select cheapest one with highest rating
    } else if (cheapestOffers.length > 1) {
        cheapestHighestRated = addAverageRatingToOffersByCompany(callback, cheapestOffers, dealsForRating);
        console.log("cheapestOffers 2-", cheapestHighestRated);
        cheapestHighestRated = findAllHighestRatedOffers(cheapestHighestRated);
        console.log("cheapestOffers 3-", cheapestHighestRated);
        if (cheapestHighestRated.length > 0) {
            theOffer = cheapestHighestRated[0];
        } else {
            theOffer = cheapestOffers[0];
        }
    }
    console.log("theOffer4-", theOffer);
    return theOffer;
}

const findCheapestOfferWithMinRating = (callback, data, minPrice, minProductRating, dealsForRating, quantityDemanded) => {
    let theOffer;
    // if (data) console.log('b4 condition3 (data)', data);
    // if (dealsForRating) console.log('b4 condition3 (dealsForRating)', dealsForRating);
    if (data && data.Items && dealsForRating && dealsForRating.Items) {
        data = addAverageRatingToOffersByCompany(callback, data, dealsForRating);
        // if (data) console.log('after addAverageRatingToOffersByCompany - data-', data);
        data.Items.map((item) => {
            console.log('in filter B4 - item.lastTenAverageRating, minProductRating', item.lastTenAverageRating, item.lastTenAverageRating.S, minProductRating);
        });
        data = {
            Items: data.Items.filter((item) => item.lastTenAverageRating >= minProductRating)
        }
        console.log('in filter After - data.Items.length', data.Items.length);
        data.Items.map((item) => {
            console.log('in filter After - item.lastTenAverageRating, minProductRating', item.lastTenAverageRating, minProductRating);
        });
        theOffer = findCheapestOffer(callback, data, dealsForRating, minPrice, quantityDemanded);
    }
    return theOffer;
}

const findHighestRatedOffer = (callback, data, minPrice, minProductRating, maxPriceFromUser, dealsForRating, quantityDemanded) => {
    let theOffer;
    data = {
        Items: data.Items.filter((item) => parseFloat(item.price.N) <= maxPriceFromUser)
    };
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
        data = {
            Items: data.Items.filter((item) => parseFloat(item.price.N) <= maxPriceFromUser)
        };
        data = addAverageRatingToOffersByCompany(callback, data, dealsForRating);
        data = {
            Items: data.Items.filter((item) => item.lastTenAverageRating >= minProductRating)
        };
        theOffer = findCheapestOffer(callback, data, dealsForRating, minPrice, quantityDemanded);
    }
    return theOffer;
}

const addAverageRatingToOffersByCompany = (callback, offersForProduct, dealsForRating) => {
    const offersForProductCount = (offersForProduct && offersForProduct.Items) ? offersForProduct.Items.length : 0;
    console.log('b4 offersForProduct -', offersForProduct, offersForProductCount);
    console.log('b4 dealsForRating -', dealsForRating, dealsForRating.Items.length);
    if (offersForProduct && offersForProduct.Items && dealsForRating && dealsForRating.Items) {
        for (let x = 0; x < offersForProduct.Items.length; x++) {
            // for (let y = 0; y < dealsForRating.Items.length; y++) {
            //     console.log('point2-', offersForProduct.Items[x].companyID.S === dealsForRating.Items[y].producerID.S, offersForProduct.Items[x].companyID, dealsForRating.Items[y].producerID);
            //     if (offersForProduct.Items[x].companyID.S === dealsForRating.Items[y].producerID.S) {
            offersForProduct.Items[x]["lastTenAverageRating"] = getLastTenAverageRating(offersForProduct.Items[x].companyID, offersForProduct.Items[x].productID, dealsForRating);
            //     }
            // }
        }
    }
    console.log('after offersForProduct -', offersForProduct, offersForProduct.length);
    return offersForProduct;
}

const getLastTenAverageRating = (coID, productID, dealsForRating) => {
    console.log('getLastTenAverageRating -coID, prodID, dealsForRating -', coID, productID, dealsForRating);
    let theRating = 0; // 0 is an initial rating for a product that does not have a rating.
    // sort out deals for this coID
    if (coID && productID && dealsForRating && dealsForRating.Items) {
        let count = 0,
            total = 0;
        let allCompanyProductRatings = [];
        dealsForRating.Items.map((deal) => {
            console.log('assignRating', deal.producerID.S == coID, deal.producerID.S, coID)
            if (deal.producerID.S == coID.S && deal.productID.S == productID.S) {
                allCompanyProductRatings.push({
                    rating: parseFloat(deal.productRatingByBuyer.N),
                    time: parseInt(deal.dealTime.N)
                });
            }
        });
        console.log('allCompanyProductRatings ', allCompanyProductRatings);
        console.log('allCompanyProductRatings sorted', allCompanyProductRatings.sort((a, b) => b.time - a.time));
        // sort result by time
        // calculate average rating

        allCompanyProductRatings.sort((a, b) => b.time - a.time).map((deal) => {
            count++;
            console.log('count++', count);
            if (count < 11) {
                total = total + deal.rating;
                console.log('in count<11 total -', total, deal.rating);
            }
        });
        console.log('total, rating ', total, theRating);
        theRating = total ? total / count : 0;
    }
    return theRating;
}

const invokeMutationsForGivenType = async (event, theOrder, theOffer, mutationType, dealsForRating) => {
    AWS.config.update({
        region: process.env.AppSyncRegion,
        credentials: new AWS.Credentials({
            accessKeyId: process.env.AccessKeyId,
            secretAccessKey: process.env.SecretAccessKey,
        })
    });


    switch (mutationType) {
        case 'NewDeal':
            const block = blockchainTheDeal(theOrder, theOffer, dealsForRating);
            await invokeAppSyncWitCustomBody(getQueryTextForNewDeal(theOrder, theOffer, block));
            await invokeAppSyncWitCustomBody(getQueryTextForUpdateOffer(theOrder, theOffer));
            await invokeAppSyncWitCustomBody(getQueryTextForUpdateOrder(theOrder, theOffer));
            await invokeAppSyncWitCustomBody(getQueryTextForNotifySeller(theOrder, theOffer, block));
            break;
        case 'Insufficient':
            await invokeAppSyncWitCustomBody(getQueryTextForInsufficientStock(theOrder, theOffer));
            break;
        case 'NotOffered':
            await invokeAppSyncWitCustomBody(getQueryTextForProductNotOffered(theOrder, theOffer));
            break;
        case 'NoneFound':
            await invokeAppSyncWitCustomBody(getQueryTextForNoneFound(theOrder));
            break;
    }
    return;
};

const getQueryTextForNewDeal = (theOrder, theOffer, block) => {
    return JSON.stringify({
        "query": "mutation ($input: CreateDealInput!) { createDeal(input: $input){  productID, dealID, orderID, buyerID, producerID, dealTime, dealPrice, dealQuantity, productRatingByBuyer, blockchainBlockID, blockchainBlockOfDeal, dealStatus, blockchainBlockStatus } }",
        "variables": {
            "input": {
                productID: theOrder.productID.S,
                dealID: new Date('January 1, 2022 00:00:00') - new Date(),
                orderID: theOrder.orderID.S,
                buyerID: theOrder.companyID.S,
                producerID: theOffer.companyID.S,
                dealTime: new Date() * 1,
                dealPrice: theOffer.price.N,
                dealQuantity: theOrder.quantity.N,
                productRatingByBuyer: null,
                blockchainBlockID: JSON.stringify(block.index),
                blockchainBlockOfDeal: JSON.stringify(block.hash),
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
                price: (parseFloat(theOffer.price.N) * ((Math.random() * 0.04) + 0.98)).toFixed(2),
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

const getQueryTextForNotifySeller = (theOrder, theOffer, block) => {
    return JSON.stringify({
        "query": "mutation ($input: CreateNotificationInput!) { createNotification(input: $input){companyID,notificationID,notificationTextRegular,notificationTextHighlighted } }",
        "variables": {
            "input": {
                companyID: theOrder.companyID.S,
                notificationID: new Date('January 1, 2022 00:00:00') - new Date(),
                notificationTextRegular: 'new blockchain block - index #' + JSON.stringify(block.index),
                notificationTextHighlighted: JSON.stringify(block)
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

const getQueryTextForNoneFound = (theOrder) => {
    return JSON.stringify({
        "query": "mutation ($input: UpdateOrderInput!) { updateOrder(input: $input){companyID,orderID,productID,product{id,name,modelNo,specificationURL,imageURL,lastTenRatingAverage},status,maxPrice,quantity,bestOfferType,secondBestOfferType,minProductRating,isCashPayment,dealPrice,note } }",
        "variables": {
            "input": {
                companyID: theOrder.companyID.S,
                orderID: theOrder.orderID.S,
                note: "none for these params",
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

const blockchainTheDeal = (theOrder, theOffer, dealsForRating) => {
    console.log('dealsForRating-', dealsForRating);
    const blankBlock = {
        index: 0,
        previousHash: "",
        hash: "",
        nonce: 0,
        productID: '',
        transactions: []
    }

    const block = blankBlock;

    const transaction = {
        buyerID: theOrder.companyID.S,
        sellerID: theOffer.companyID.S,
        quantity: theOrder.quantity.N,
        price: theOffer.price.N,
        productID: theOrder.productID.S
    }

    const transactions = [...dealsForRating.Items, transaction];
    console.log('transaction, transactions', transaction, transactions);

    if ((dealsForRating.Items && dealsForRating.Items.length == 0) || !dealsForRating.Items) {
        block.previousHash = "0000000000000000";
        block.hash = sha256(JSON.stringify(transactions) + block.index + block.previousHash + block.nonce);
        block.productID = theOrder.productID.S;
    } else if (dealsForRating.Items && dealsForRating.Items.length > 0) {
        let previousBlock = dealsForRating.Items[0].blockchainBlockOfDeal.S;
        block.index = dealsForRating.Items.length;
        block.previousHash = previousBlock;
        block.hash = sha256(JSON.stringify(transactions) + block.index + block.previousHash + block.nonce);
        block.productID = theOrder.productID.S;
    }
    return block
}
/**
 * @description 获取cryptocompare的实时数据
 * @author djo0x0 <mod@heiljo.com>
 *
 * 
 *
 *  行情redis数据结构
 *  
 *  SortedSet:
 *
 *  交易市场列表：{version}:exchange:list
 *  货币列表：{version}:coin:list
 *  交易对列表：{version}:coin:{coin_name}:pair
 *
 *
 *  hash:
 *
 *  交易市场详情：{version}:exchange:{exchange_code}
 *  exch_id
 *  excha_name
 *  exch_code
 *  
 * 
 *  货币详情：{version}:coin:{coin_name}
 *  coin_id
 *  coin_name
 *  pair
 *
 *  交易市场内交易对的行情：{version}:exchange:{exchange_code}:{pair}:data
 *  
 * 
 */


var io      = require('socket.io-client');
var CCC     = require('./tmtsock.js');
var socket  = io.connect('https://streamer.cryptocompare.com/', {reconnect: true});
var redis   = require('redis');
var request = require('request');

/**
 * 连接redis
 * @type {boolean}
 */
var rcli    = redis.createClient(6379, 'localhost');

/**
 * 获取实时交易数据socket频道API
 * @type {String}
 */
var subHost = 'https://min-api.cryptocompare.com/data/subs';


/**
 * 设置交易数据版本号
 * @type {String}
 */
var verison = 'dg1521772314';


/**
 * 设置交易数据存储的redis db库
 */
rcli.select(5);

/**
 * 需要获取统计数据的交易对列表
 * @type {Array}
 */
var subscription = [];




/**
 * 获取所有的货币列表
 * @return {array} 
 */
function getCoinList(){
    return new Promise((resolve, reject) => {
        rcli.zrange([verison+':coin:list', 0, -1], function(err, response) {
            if(err) {
                reject(err)
            } else {
                resolve(response)
            }
        });
    })
}


/**
 * 根据货币代号获取它支持的交易对列表
 * @param  {string} coin
 * @return {array}
 */
function getPairList(coin) {

    return new Promise((resolve,reject) => {        
        rcli.zrange([verison+':coin:'+coin+':pair', 0, -1], function(err, response) {
            if (err) {
                reject(err);
            }else {
                resolve(response);
            }
        });
    });
}


/**
 * 生成socket支持的交易对监听频道名称
 * @param  {array} coinList
 * @return {boolean}
 */
function getPair(coinList) {
    return new Promise((resolve, reject) => {
        let res = coinList.map(coin => {
            return getPairList(coin).then(pairList => {
                pairList.map(pair => {
                    pairArr = pair.toString().split("/");
                    channel = '5~CCCAGG~'+pairArr[0]+'~'+pairArr[1];
                    subscription.push(channel);
                }) 
            }).catch();
        }) 
        Promise.all(res).then(_ => {
            resolve()
        })
    })
}


/**
 * 根据交易对获取其支持的实时数据监听频道名称
 * @return {array}
 */
function getLiveDataChannel(){
    var fsym    = "BTC";
    var tsym    = "USD";
    var dataUrl = subHost + "?fsym=" + fsym + "&tsyms=" + tsym;
    return new Promise((resolve, reject) => {
        request(dataUrl, { json: true }, (err, res, data) => {
            if (err) { 
                reject(err);
            }else {
                resolve(data);
            }
        });    
    });
}


/**
 * 同步实时交易数据，区分平台 trade
 * @param  {json} currentData
 * @return {stream}
 */
function subscibeCurrent(currentData) {
    socket.on('m', function(currentData) {
        var tradeField = currentData.substr(0, currentData.indexOf("~"));
        if (tradeField == CCC.STATIC.TYPE.TRADE) {
            d        = transformData(currentData);
            exch     = d.Market.toLowerCase();
            fsym     = d.Fsym;
            tsym     = d.Tsym;
            type     = d.Type;
            price    = d.Price;
            quantity = d.Quantity;
            total    = d.Total;
            if (type == 'BUY' || type == 'SELL') {
                // {version}:exchange:{exchange_code}:{pair}:livedata
                hashkey = verison +':exchange:'+ exch + ':' + fsym + '/' + tsym + ':livedata';
                rcli.hmset(hashkey, {
                    'exch_code': exch,
                    'fsym': fsym,
                    'tsym': tsym,
                    'price': price,
                    'total': total,
                    'quantity': quantity,
                    'type': type
                });
                rcli.hgetall(hashkey, function (err, obj) {
                    console.dir(obj);
                });
            }
        }
    });
}


/**
 * 解析数据格式
 * @param  {json} data
 * @return {array}
 */
function transformData (data) {
    var incomingTrade = CCC.TRADE.unpack(data);
    console.log(incomingTrade);
    var newTrade = {
        Market: incomingTrade['M'],
        Type: incomingTrade['T'],
        ID: incomingTrade['ID'],
        Price: incomingTrade['P'],
        Quantity: incomingTrade['Q'],
        Total: incomingTrade['TOTAL'],
        Fsym: incomingTrade['FSYM'],
        Tsym: incomingTrade['TSYM']
        // Price: CCC.convertValueToDisplay(cointsym, incomingTrade['P']),
        // Quantity: CCC.convertValueToDisplay(coinfsym, incomingTrade['Q']),
        // Total: CCC.convertValueToDisplay(cointsym, incomingTrade['TOTAL'])
    };

    if (incomingTrade['F'] & 1) {
        newTrade['Type'] = "SELL";
    }
    else if (incomingTrade['F'] & 2) {
        newTrade['Type'] = "BUY";
    }
    else {
        newTrade['Type'] = "UNKNOWN";
    }

    return newTrade;
};




/**
 * 同步数据货币的汇总数据，不区分平台
 * @return {stream}
 */
function subscibe(){
    // Add a connect listener
    socket.on('connect', function (socket) {
        console.log('Connected!');
    });

    //Add channel
    subscription.map(function(item) {
        let sin = [];
        sin.push(item);
        socket.emit('SubAdd', { subs: sin });
    });

    // socket.emit('SubAdd', { subs: subscription });
    socket.on('m', function(message){
        var messageType = message.substring(0, message.indexOf("~"));
        var res = {};
        if (messageType == CCC.STATIC.TYPE.CURRENTAGG) {
        res = CCC.CURRENT.unpack(message);
            console.log(res);
        }
    });
    socket.on('disconnect', function(){
        console.log('Disconnected!');
    });
}


//同步交易对的统计数据
getCoinList().then(getPair).then(_ => {
    subscibe();
});


//同步交易对在各个交易所的实时交易数据
getLiveDataChannel().then(data => {
    currentSubs = data['USD']['TRADES'];
    console.log(currentSubs);
    var currentSubsText = "";
    for (var i = 0; i < currentSubs.length; i++) {
        currentSubsText += currentSubs[i] + ", ";
    }
    socket.emit('SubAdd', { subs: currentSubs });
}).then(_ => {
    subscibeCurrent();
});

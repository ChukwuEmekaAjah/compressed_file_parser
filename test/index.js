const assert = require('assert');
const {parseRow, processChunk, processFeed} = require('../index');
const fs = require('fs');
const path = require('path');

describe('Testing of individual data row parsing and validation', function(){
    it("Should return 'null' for file row that availability is 'out of stock'", function(){
        const outOfStock = {
            "id":"70395",
            "title":"Rustic Plastic Bike",
            "description":"Pizza",
            "link":"https://else.name",
            "image_link":"http://lorempixel.com/640/480",
            "brand":"Skiles - Fahey",
            "price":"590.00",
            "availability":"out of stock"
        }
        let config = {
            totalRowCount: 0,
            removedRowCount: 0,
            minPrice : Infinity,
            maxPrice : -Infinity,
        }
        assert.strictEqual(parseRow(JSON.stringify(outOfStock), config), null)
    })
    
    it("Should return 'null' for file row that has 'Collier' in brand column ", function(){
        const collierBrands = [{
            "id":"70395",
            "title":"Rustic Plastic Bike",
            "description":"Pizza",
            "link":"https://else.name",
            "image_link":"http://lorempixel.com/640/480",
            "brand":"Skiles Collier - Fahey",
            "price":"590.00",
            "availability":"out of stock"
        }, {
            "id":"70395",
            "title":"Rustic Plastic Bike",
            "description":"Pizza",
            "link":"https://else.name",
            "image_link":"http://lorempixel.com/640/480",
            "brand":"Collier - Fahey",
            "price":"590.00",
            "availability":"out of stock"
        }, {
            "id":"70395",
            "title":"Rustic Plastic Bike",
            "description":"Pizza",
            "link":"https://else.name",
            "image_link":"http://lorempixel.com/640/480",
            "brand":"Collier     ",
            "price":"590.00",
            "availability":"out of stock"
        }]
        let config = {
            totalRowCount: 0,
            removedRowCount: 0,
            minPrice : Infinity,
            maxPrice : -Infinity,
        }
    
        for(let collier of collierBrands){
            assert.strictEqual(parseRow(JSON.stringify(collier), config), null)
        }
    })
    
    it("Should not return 'null' for file row that has availability of 'in stock'", function(){
        const inStockProducts = [{
            "id":"70395",
            "title":"Rustic Plastic Bike",
            "description":"Pizza",
            "link":"https://else.name",
            "image_link":"http://lorempixel.com/640/480",
            "brand":"Skiles Fahey",
            "price":"590.00",
            "availability":"in stock"
        }]
        let config = {
            totalRowCount: 0,
            removedRowCount: 0,
            minPrice : Infinity,
            maxPrice : -Infinity,
        }
    
        for(let product of inStockProducts){
            const parsedRow = parseRow(JSON.stringify(product), config)
            assert.strictEqual(parsedRow.id, product.id)
            assert.strictEqual(parsedRow.title, product.title)
            assert.strictEqual(parsedRow.availability, product.availability)
        }
    })
    
    it("Should format data row price with currency", function(){
        const inStockProducts = [{
            "id":"70395",
            "title":"Rustic Plastic Bike",
            "description":"Pizza",
            "link":"https://else.name",
            "image_link":"http://lorempixel.com/640/480",
            "brand":"Skiles Fahey",
            "price":"590.00",
            "availability":"in stock"
        }]
    
        let config = {
            totalRowCount: 0,
            removedRowCount: 0,
            minPrice : Infinity,
            maxPrice : -Infinity,
        }
    
        for(let product of inStockProducts){
            const formattedRow = parseRow(JSON.stringify(product), config)
            assert.strictEqual(formattedRow.price, `$${product.price} USD`)
            
        }
    })
    
    
    it("Should make size of processed file smaller - Size of compressed processed file should be smaller than original", function(){
        
        const commerceFeedStat = fs.statSync(`${process.cwd()}${path.sep}commerce-feed.csv.gz`);
        const processedFeedStat = fs.statSync(`${process.cwd()}${path.sep}processed.csv.gz`);
        assert.strictEqual(commerceFeedStat.size > processedFeedStat.size, true);
    })
})


describe('Testing of chunk processing', function(){

    this.afterAll(function(done){
        fs.unlinkSync(`${process.cwd()}${path.sep}test_processed.csv`)
        done()
    });

    it("Should return total rows and removed rows from read data chunk", function(){
        const chunkData = `{"id":"70395","title":"Rustic Plastic Bike","description":"Pizza","link":"https://else.name","image_link":"http://lorempixel.com/640/480","brand":"Skiles Fahey","price":"50.00","availability":"in stock"}\n{"id":"7395","title":"Rustic Plastic Bike","description":"Pizza","link":"https://else.name","image_link":"http://lorempixel.com/640/480","brand":"Skiles Fahey","price":"590.00","availability":"out of stock"}\n{"id":"70395","title":"Rustic Plastic Bike","description":"Pizza","link":"https://else.name","image_link":"http://lorempixel.com/640/480","brand":"Skiles Fahey","price":"590.00","availability":"in stock"}\n{"id":"703395","title":"Rustic Plastic Bike","description":"Pizza","link":"https://else.name","image_link":"http://lorempixel.com/640/480","brand":"Skiles Fahey","price":"190.00","availability":"in stock"}`
    
        const tempFile = fs.createWriteStream(`${process.cwd()}${path.sep}test_processed.csv`);
        
        let parserConfig = {
            writtenHeader: false,
            tempFile,
            lastRow: null,
            fileEnding: false,
            chunk: chunkData,
            totalRowCount: 0,
            removedRowCount: 0,
            minPrice : Infinity,
            maxPrice : -Infinity,
        }
    
        let processedChunkResult = processChunk(parserConfig);
        assert.strictEqual(processedChunkResult.removedRowCount, 1)

        // simulate end of stream event
        parserConfig.fileEnding = true;
        processedChunkResult = processChunk(parserConfig);
        assert.strictEqual(processedChunkResult.totalRowCount, 4)

    })

    it("Should return correct min and max prices", function(){
        const minPrice = 50.00, maxPrice =  1590.0;
        const chunkData = `{"id":"70395","title":"Rustic Plastic Bike","description":"Pizza","link":"https://else.name","image_link":"http://lorempixel.com/640/480","brand":"Skiles Fahey","price":"${minPrice}","availability":"in stock"}\n{"id":"7395","title":"Rustic Plastic Bike","description":"Pizza","link":"https://else.name","image_link":"http://lorempixel.com/640/480","brand":"Skiles Fahey","price":"590.00","availability":"out of stock"}\n{"id":"70395","title":"Rustic Plastic Bike","description":"Pizza","link":"https://else.name","image_link":"http://lorempixel.com/640/480","brand":"Skiles Fahey","price":"590.00","availability":"in stock"}\n{"id":"703395","title":"Rustic Plastic Bike","description":"Pizza","link":"https://else.name","image_link":"http://lorempixel.com/640/480","brand":"Skiles Fahey","price":"${maxPrice}","availability":"in stock"}`
    
        const tempFile = fs.createWriteStream(`${process.cwd()}${path.sep}test_processed.csv`);
        
        let parserConfig = {
            writtenHeader: false,
            tempFile,
            lastRow: null,
            fileEnding: false,
            chunk: chunkData,
            totalRowCount: 0,
            removedRowCount: 0,
            minPrice : Infinity,
            maxPrice : -Infinity,
        }
    
        let processedChunkResult = processChunk(parserConfig);
        
        // simulate end of stream event
        parserConfig.fileEnding = true;
        processedChunkResult = processChunk(parserConfig);
        
        assert.strictEqual(parserConfig.minPrice, minPrice)
        assert.strictEqual(parserConfig.maxPrice, maxPrice)
    })
    
})
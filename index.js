/*
 * processFeed is the entrypoint to this test but feel free
 * to break up your code in whatever way makes sense to you
 */
const fs = require("fs");
const zlib = require("zlib");
const csv = require("csvtojson");
const path = require("path");

function parseRow(row, config) {
	config.totalRowCount += 1;
	
  	let parsedRow;
  	try {
    	parsedRow = JSON.parse(`${row}`);
  	} catch (exc) {
    	return null;
  	}

 	if ((parsedRow.brand && parsedRow.brand.trim().toLowerCase()).indexOf("collier") > -1) {
    	config.removedRowCount += 1;
    	return null;
  	}

  	if ((parsedRow.availability && parsedRow.availability.trim().toLowerCase()) === "out of stock" ) {
    	config.removedRowCount += 1;
    	return null;
  	}

  	// clean price
  	parsedRow.price = parsedRow.price.replace(/[^0-9.]/gim, ""); //remove non-numeric characters from price
  	if (isNaN(parsedRow.price.trim())) {
   		config.removedRowCount += 1;
    	return null;
  	}
	
  	config.minPrice = Number(parsedRow.price) < config.minPrice ? Number(parsedRow.price) : config.minPrice;
  	config.maxPrice = Number(parsedRow.price) > config.maxPrice ? Number(parsedRow.price) : config.maxPrice;

 	parsedRow.price = `$${parsedRow.price.trim()} USD`; // format price to specification
	
	return parsedRow
}

function processChunk(config){
    if(config.fileEnding){ // update file if this is the last row of data.
        if(config.lastRow){
            let dataRow = parseRow(config.lastRow.trim(), config);
            if (dataRow) config.tempFile.write(`${Object.values(dataRow).join(", ")}`);
            return {totalRowCount:config.totalRowCount, removedRowCount: config.removedRowCount};
        }
        return {totalRowCount:config.totalRowCount, removedRowCount: config.removedRowCount};
    }
	
	
	if (!config.chunk) return null;
	config.chunk = config.chunk.toString();
	if (config.lastRow) {
		config.chunk = config.lastRow + config.chunk;
	}

	config.chunk = config.chunk.replace(/\r/gim, "");
	const parts = config.chunk.split(/\n|\n\r/gim);
	
	for (let i = 0; i < parts.length - 1; i++) {
	
		let dataRow = parseRow(parts[i].trim(), config);
		
		if (dataRow){
			if(!config.writtenHeader){ // write csv file header row
				config.tempFile.write( `${Object.keys(dataRow).join(", ")}\n`)
				config.writtenHeader = true;
			}

			config.tempFile.write( `${Object.values(dataRow).join(", ")}\n`);
		} 
	}

	config.lastRow = parts[parts.length - 1]; // data is not read row by row by stream, multiple rows and some parts of a row can be in a chunk of data.
    return {totalRowCount:config.totalRowCount, removedRowCount: config.removedRowCount};
}

function processFeed() {
	console.log("Processing...")
  	const inputFile = fs.createReadStream(`${__dirname}${path.sep}commerce-feed.csv.gz`);
  	const intermediateFile = fs.createWriteStream(`${__dirname}${path.sep}intermediate.csv`);
  	const tempFile = fs.createWriteStream(`${__dirname}${path.sep}processed.csv`);
	const outputFile = fs.createWriteStream(`${__dirname}${path.sep}processed.csv.gz`);
	
	let parserConfig = {
		writtenHeader: false, // csv column headers for starting the file
		tempFile, // the temporary file which csv parser result was written to
		lastRow: null, // last row of data in each chunk of data read
		fileEnding: false, // indicator of the end of reading of a stream
		chunk: null, // chunk of data read on each data read event
		totalRowCount: 0, // total number of rows in compressed file
		removedRowCount: 0, // total number of removed rows in compressed file
		minPrice : Infinity, // minimum price of item in csv file
		maxPrice : -Infinity, // maximum price of item in csv file
	}

  	inputFile
		.pipe(zlib.createGunzip()) // unzip file
		.pipe(csv()) // parse file from csv format to json string
    	.pipe(intermediateFile) // write csv content to intermediate file
    	.on("finish", function () {
      		const processedFile = fs.createReadStream(`${__dirname}${path.sep}intermediate.csv`);
      		
			processedFile.on("data", function (chunk) {
				parserConfig.chunk = chunk;
        		processChunk(parserConfig)
      		});

      		processedFile.on("end", function () {
        		parserConfig.fileEnding = true;
				processChunk(parserConfig)
        		
				fs.unlinkSync(`${__dirname}${path.sep}intermediate.csv`); // delete intermediate file

        		const tempFile = fs.createReadStream(`${__dirname}${path.sep}processed.csv`);
        		tempFile
					.pipe(zlib.createGzip()) // zip file
          			.pipe(outputFile)
          			.on("finish", function () {
            			fs.unlinkSync(`${__dirname}${path.sep}processed.csv`);// delete temporary file

						// log report
						console.log(`Total Row Count: ${parserConfig.totalRowCount}\nRemoved Row Count: ${parserConfig.removedRowCount}\nMax Price: ${parserConfig.maxPrice}\nMin Price: ${parserConfig.minPrice}`);
          			});
        		
				
     		});
   		});
}

if (require.main === module) {
	processFeed();
}

module.exports = {
	parseRow,
    processChunk,
	processFeed,
}

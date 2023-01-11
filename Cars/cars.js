const express = require('express');
const { Pool } = require('pg');
const cars = express();
const bodyParser = require('body-parser');

const path = '/api/v1';
const serverPortNumber = 8070;
const errors = {error404: {message: 'Not found Person for ID'},
				error400: {message: 'Invalid data'},
}

const pool = new Pool({
	user: 'program',
	database: 'cars',
	password: 'test',
	port: 5432,
	host: 'postgres',
});

cars.use(bodyParser.json());
cars.use(bodyParser.urlencoded({
  extended: true
}));

setTimeout(() => {
	tableInit();
}, 2000);

cars.get('/manage/health', (request, response) => {
	pool.query('SELECT * FROM cars', (err, res) => {
		if(!err){
		  console.log(res);
		  response.status(200).send();
		}
		else {
		  console.log(err);
		  response.status(200).send();
		}
	})
});

cars.get(path+'/cars', (request, response) => {
	let getAllCarsQuery = `
	SELECT * FROM cars
	`
	
	getAllCarsQuery += (request.query.showAll == false ? ' WHERE available = true;' : ';');
	
	pool.query(getAllCarsQuery)
		.then(res => {
			let resObject = {
				page: +request.query.page,
				pageSize: +request.query.size,
				totalElements: 0,
				items: []
			}
			
			resObject.items = res.rows.slice((resObject.page-1) * resObject.pageSize, resObject.page * resObject.pageSize);
			resObject.totalElements = resObject.items.length;
			
			for(let i = 0; i < resObject.items.length; i++){
				resObject.items[i].carUid = resObject.items[i]['car_uid'];
				delete resObject.items[i].car_uid;
				resObject.items[i].registrationNumber = resObject.items[i]['registration_number'];
				delete resObject.items[i].registration_number;
			}
			
			response.status(200).json(resObject);
		})
});

cars.post(path+'/carcheck', (request, response) => {
	let getCarByUidQuery = `
	SELECT * FROM cars WHERE car_uid = $1 AND available = true;
	`;
	let availableUpdateQuery = `
	UPDATE cars SET available = false WHERE car_uid = $1;
	`;
	
	pool.query(getCarByUidQuery, Object.values(request.body))
		.then(result => {
			if (result.rows.length > 0) {
				pool.query(availableUpdateQuery, Object.values(request.body))
					.then(res => {
						let resObject = {
							price: (result.rows[0]).price
						};
						response.status(200).json(resObject);
					})
			} else {
				response.sendStatus(400);
			}
		})
});

cars.post(path+'/cars_by_uid', (request, response) => {
	let getCarByUidQuery = `
	SELECT car_uid, brand, model, registration_number FROM cars 
	WHERE car_uid IN (\'`+((request.body.carsUidsArr).join('\',\''))+`\');
	`;
	
	pool.query(getCarByUidQuery)
		.then(result => {
			let resObj = {};
			for(let obj of result.rows) {
				resObj[obj.car_uid] = obj;
			}
			
			response.status(200).json(resObj);
		})
});

cars.put(path+'/free_car', (request, response) => {
	let freeCarQuery = `
	UPDATE cars SET available = true WHERE car_uid = $1;
	`;
	
	pool.query(freeCarQuery, [request.query.carUid])
		.then(res => {
			response.sendStatus(200);
		})
})

cars.listen(process.env.PORT || serverPortNumber, () => {
	console.log('Cars server works on port '+serverPortNumber);
})

function tableInit() {
	let carsTable = `
	CREATE TABLE cars
	(
		id                  SERIAL PRIMARY KEY,
		car_uid             uuid UNIQUE NOT NULL,
		brand               VARCHAR(80) NOT NULL,
		model               VARCHAR(80) NOT NULL,
		registration_number VARCHAR(20) NOT NULL,
		power               INT,
		price               INT         NOT NULL,
		type                VARCHAR(20)
			CHECK (type IN ('SEDAN', 'SUV', 'MINIVAN', 'ROADSTER')),
		available        BOOLEAN     NOT NULL
	);
	`;
	
	let testValues = [
		1,
		'109b42f3-198d-4c89-9276-a7520a7120ab',
		'Mercedes Benz',
		'GLA 250',
		'ЛО777Х799',
		249,
		3500,
		'SEDAN',
		true
	]
	
	let testDataInsert = `
		INSERT INTO cars (id, car_uid, brand, model, registration_number, power, price, type, available) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
	`

	pool.query(carsTable)
		.then(res => {
			console.log('Table initialized');
			pool.query(testDataInsert, testValues, (err, res) => {
				if(!err){
				  console.log('Data inserted')
				}
				else {
				  console.log('Data not inserted')
				}
			})
		})
		.catch(err => {
			console.log('Table initialization error');
		})
}
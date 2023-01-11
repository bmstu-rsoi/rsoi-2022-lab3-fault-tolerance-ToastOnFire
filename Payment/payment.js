const express = require('express');
const { Pool } = require('pg');
const payment = express();
const bodyParser = require('body-parser');

const path = '/api/v1';
const serverPortNumber = 8050;
const errors = {error404: {message: 'Not found Person for ID'},
				error400: {message: 'Invalid data'},
}

const pool = new Pool({
	user: 'program',
	database: 'payments',
	password: 'test',
	port: 5432,
	host: 'postgres',
});

payment.use(bodyParser.json());
payment.use(bodyParser.urlencoded({
  extended: true
})); 

setTimeout(() => {
	tableInit();
}, 2000);

payment.get('/manage/health', (request, response) => {
  response.status(200).send();
});

payment.post(path+'/payment/add', (request, response) => {
	let addRentalQuery = `
	INSERT INTO payment (payment_uid, status, price) 
	VALUES ($1, 'PAID', $2);
	`
	
	let dateFrom = new Date(request.body.dateFrom);
	let dateTo = new Date(request.body.dateTo);
	
	let values = [request.body.paymentUid, Math.ceil(request.body.price*(Math.abs(dateTo.getTime() - dateFrom.getTime()) / (1000 * 3600 * 24)))]
	
	pool.query(addRentalQuery, values)
		.then(res => {
			response.sendStatus(200);
		}).catch(err => {
			response.sendStatus(400);
		})
});

payment.post(path+'/payment_by_uid', (request, response) => {
	let getPaymentsByUidQuery = `
	SELECT payment_uid, status, price FROM payment 
	WHERE payment_uid IN (\'`+((request.body.paymentUidsArr).join('\',\''))+`\');
	`;
	
	pool.query(getPaymentsByUidQuery)
		.then(result => {
			resObj = {};
			for(let obj of result.rows) {
				resObj[obj.payment_uid] = obj;
			}
			response.status(200).json(resObj);
		})
});

payment.put(path+'/cancel_payment', (request, response) => {
	let cancelPaymentQuery = `
	UPDATE payment SET status ='CANCELED' WHERE payment_uid = $1;
	`;
	
	pool.query(cancelPaymentQuery, [request.query.paymentUid])
		.then(res => {
			response.sendStatus(204);
		})
})

payment.listen(process.env.PORT || serverPortNumber, () => {
	console.log('Payment server works on port '+serverPortNumber);
})

function tableInit() {
	let carsTable = `
	CREATE TABLE payment
	(
		id          SERIAL PRIMARY KEY,
		payment_uid uuid        NOT NULL,
		status      VARCHAR(20) NOT NULL
			CHECK (status IN ('PAID', 'CANCELED')),
		price       INT         NOT NULL
	);
	`;

	pool.query(carsTable)
		.then(res => {
			console.log('Table initialized')
		})
		.catch(err => {
			console.log('Table initialization error');
		})
}
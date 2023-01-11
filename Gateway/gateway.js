const express = require('express');
const { v4: uuidv4 } = require('uuid');
const gateway = express();
const bodyParser = require('body-parser');

const path = '/api/v1';
const adress = {
	cars: 'http://cars:8070',
	payment: 'http://payment:8050',
	rental: 'http://rental:8060',
}
const serverPortNumber = 8080;
const errors = {error404: {message: 'Not found Person for ID'},
				error400: {message: 'Invalid data'},
}

gateway.use(bodyParser.json());
gateway.use(bodyParser.urlencoded({
  extended: true
})); 

gateway.get('/manage/health', (request, response) => {
	response.status(200).send();
});

gateway.get(path+'/cars', (request, response) => {
	let carsParams = {
		page: request.query.page,
		size: request.query.size,
		showAll: request.query.showAll
	}
	
	fetch(adress.cars+path+'/cars?' + new URLSearchParams(carsParams), {
		method: 'GET'
	})
	.then(result => result.json())
    .then(data => response.status(200).json(data));
});

gateway.get(path+'/rental', (request, response) => {
	let userName = {
		username: request.header('X-User-Name')
	}
	
	fetch(adress.rental+path+'/rental_by_params?' + new URLSearchParams(userName), {
		method: 'GET'
	})
	.then(result => result.json())
	.then(resData => {
		let carsUids = [];
		let paymentUids = [];
		
		for(let obj of resData) {
			carsUids.push(obj.car_uid);
			paymentUids.push(obj.payment_uid);
		}
		
		Promise.all([
			fetch(adress.cars+path+'/cars_by_uid', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({carsUidsArr: carsUids})
			}),
			fetch(adress.payment+path+'/payment_by_uid', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({paymentUidsArr: paymentUids})
			})
		])
		.then(resArr => {
			return Promise.all(resArr.map(r => r.json()));
		})
		.then(resArrData => {
			responseArray = [];
			for(let obj of resData) {
				let responseObj = {
					rentalUid: obj.rental_uid,
					status: obj.status,
					dateFrom: obj.date_from,
					dateTo: obj.date_to,
					car: {
						carUid: obj.car_uid,
						brand: (resArrData[0][obj.car_uid]).brand,
						model: (resArrData[0][obj.car_uid]).model,
						registrationNumber: (resArrData[0][obj.car_uid]).registration_number
					},
					payment: {
						paymentUid: obj.payment_uid,
						status: (resArrData[1][obj.payment_uid]).status,
						price: (resArrData[1][obj.payment_uid]).price
					}
				}
				responseArray.push(responseObj);
			}
			
			response.status(200).json(responseArray);
		})
	})
});

gateway.get(path+'/rental/:rentalUid', (request, response) => {
	let rentalParams = {
		username: request.header('X-User-Name'),
		rentalUid: request.params.rentalUid
	}
	
	fetch(adress.rental+path+'/rental_by_params?' + new URLSearchParams(rentalParams), {
		method: 'GET'
	})
	.then(result => result.json())
	.then(resData => {
		let carsUids = [];
		let paymentUids = [];
		
		if (resData.length > 0) {
			carsUids.push(resData[0].car_uid);
			paymentUids.push(resData[0].payment_uid);
		} else {
			response.status(404).json({message: 'Билет не найден'});
		}
		
		Promise.all([
			fetch(adress.cars+path+'/cars_by_uid', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({carsUidsArr: carsUids})
			}),
			fetch(adress.payment+path+'/payment_by_uid', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({paymentUidsArr: paymentUids})
			})
		])
		.then(resArr => {
			return Promise.all(resArr.map(r => r.json()));
		})
		.then(resArrData => {
			let responseObj = {
				rentalUid: resData[0].rental_uid,
				status: resData[0].status,
				dateFrom: resData[0].date_from,
				dateTo: resData[0].date_to,
				car: {
					carUid: carsUids[0],
					brand: (resArrData[0][carsUids[0]]).brand,
					model: (resArrData[0][carsUids[0]]).model,
					registrationNumber: (resArrData[0][carsUids[0]]).registration_number
				},
				payment: {
					paymentUid: paymentUids[0],
					status: (resArrData[1][paymentUids[0]]).status,
					price: (resArrData[1][paymentUids[0]]).price
				}
			}
			
			response.status(200).json(responseObj);
		})
	})
});

gateway.post(path+'/rental', (request, response) => {
	let rentalParams = {
		rentalUid: uuidv4(),
		username: request.header('X-User-Name'),
		paymentUid: undefined,
		carUid: request.body.carUid,
		dateFrom: request.body.dateFrom,
		dateTo: request.body.dateTo
	}
	
	let carsParams = {
		carUid: rentalParams.carUid
	}
	
	fetch(adress.cars+path+'/carcheck', {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json'
		},
		body: JSON.stringify(carsParams)
	})
	.then(result => {
		if (result.status == 200) {
			return result.json();
		} else {
			return 400;
		}
	})
	.then(resultData => {
		if (resultData != 400) {
			let paymentParams = {
				price: resultData.price,
				dateFrom: request.body.dateFrom,
				dateTo: request.body.dateTo,
				paymentUid: uuidv4()
			}
			
			rentalParams.paymentUid = paymentParams['paymentUid'];
			
			Promise.all([
				fetch(adress.rental+path+'/rental/add', {
					method: 'POST',
					headers: {
						'Content-Type': 'application/json'
					},
					body: JSON.stringify(rentalParams)
				}),
				fetch(adress.payment+path+'/payment/add', {
					method: 'POST',
					headers: {
						'Content-Type': 'application/json'
					},
					body: JSON.stringify(paymentParams)
				})
			]).then(resArr => {
				if(resArr[0].status == 200 && resArr[1].status == 200) {
					let dateFrom = new Date(request.body.dateFrom);
					let dateTo = new Date(request.body.dateTo);
					
					responseObj = {
						rentalUid: rentalParams.rentalUid,
						status: 'IN_PROGRESS',
						carUid: rentalParams.carUid,
						dateFrom: rentalParams.dateFrom,
						dateTo: rentalParams.dateTo,
						payment: {
							paymentUid: paymentParams.paymentUid,
							status: 'PAID',
							price: Math.ceil(paymentParams.price * (Math.abs(dateTo.getTime() - dateFrom.getTime()) / (1000 * 3600 * 24)))
						}
					}
					
					response.status(200).json(responseObj)
				} else {
					response.status(400).json({message: 'Ошибка: не получилось создать записи об аренде'})
				}
			})
		}  else {
			response.status(400).json({message: 'Ошибка: Автомобиль уже забронирован'})
		}
	})
});

gateway.delete(path+'/rental/:rentalUid', (request, response) => {
	let rentalGetParams = {
		username: request.header('X-User-Name'),
		rentalUid: request.params.rentalUid
	}
	
	fetch(adress.rental+path+'/get_rental_uids?'+new URLSearchParams(rentalGetParams), {
		method: 'GET'
	})
	.then(result => {
		if (result.status == 200) {
			return result.json();
		} else {
			return 404;
		}
	})
	.then(resData => {
		if (resData != 404) {
			let rentalPutParams = {
				status: 'CANCELED',
				rentalUid: rentalGetParams.rentalUid
			}
			Promise.all([
				fetch(adress.rental+path+'/change_status', {
					method: 'PUT',
					headers: {
						'Content-Type': 'application/json'
					},
					body: JSON.stringify(rentalPutParams)
				}),
				fetch(adress.cars+path+'/free_car?'+new URLSearchParams({carUid: resData.car_uid}), {
					method: 'PUT'
				}),
				fetch(adress.payment+path+'/cancel_payment?'+new URLSearchParams({paymentUid: resData.payment_uid}), {
					method: 'PUT'
				})
			])
			.then(putsRes => {
				response.sendStatus(204);
			})
		} else {
			response.status(404).json({message: 'Аренда не найдена'});
		}
	})
});

gateway.post(path+'/rental/:rentalUid/finish', (request, response) => {
	let rentalGetParams = {
		username: request.header('X-User-Name'),
		rentalUid: request.params.rentalUid
	}
	
	fetch(adress.rental+path+'/get_rental_uids?'+new URLSearchParams(rentalGetParams), {
		method: 'GET'
	})
	.then(result => {
		if (result.status == 200) {
			return result.json();
		} else {
			return 404;
		}
	})
	.then(resData => {
		if (resData != 404) {
			let rentalPutParams = {
				status: 'FINISHED',
				rentalUid: rentalGetParams.rentalUid
			}
			Promise.all([
				fetch(adress.rental+path+'/change_status', {
					method: 'PUT',
					headers: {
						'Content-Type': 'application/json'
					},
					body: JSON.stringify(rentalPutParams)
				}),
				fetch(adress.cars+path+'/free_car?'+new URLSearchParams({carUid: resData.car_uid}), {
					method: 'PUT'
				})
			])
			.then(putsRes => {
				response.sendStatus(204);
			})
		} else {
			response.status(404).json({message: 'Аренда не найдена'});
		}
	})
});

gateway.listen(process.env.PORT || serverPortNumber, () => {
	console.log('Gateway server works on port '+serverPortNumber);
})
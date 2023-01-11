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

circuitBreakerObj = {
	maxAttempts: 3,
	cars: 0,
	payment: 0,
	rental: 0,
	checkConnection: {
		cars: undefined,
		payment: undefined,
		rental: undefined
	},
	requestQueue: new Array()
}

gateway.get('/manage/health', (request, response) => {
	response.status(200).send();
});


// Список всех автомобилей
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
    .then(data => response.status(200).json(data))
	.catch(error => response.sendStatus(500));
});

// Получить информацию о всех арендах пользователя
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
		
		makeRequest(adress.cars+path+'/cars_by_uid', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({carsUidsArr: carsUids})
		}, 'cars')
		.then(resCars => {
			makeRequest(adress.payment+path+'/payment_by_uid', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({paymentUidsArr: paymentUids})
			}, 'payment')
			.then(resPayment => {
				responseArray = [];
				for(let obj of resData) {
					let responseObj = {
						rentalUid: obj.rental_uid,
						status: obj.status,
						dateFrom: obj.date_from,
						dateTo: obj.date_to,
						car: undefined,
						payment: undefined
					}
					
					if(resCars != 500) {
						responseObj.car = {
							carUid: obj.car_uid,
							brand: (resCars[obj.car_uid]).brand,
							model: (resCars[obj.car_uid]).model,
							registrationNumber: (resCars[obj.car_uid]).registration_number
						}
					} else {
						responseObj.car = obj.car_uid
					}
					
					if(resPayment != 500) {
						responseObj.payment =  {
							paymentUid: obj.payment_uid,
							status: (resPayment[obj.payment_uid]).status,
							price: (resPayment[obj.payment_uid]).price
						}
					} else {
						responseObj.payment = {}
					}
					
					responseArray.push(responseObj);
				}
				
				response.status(200).json(responseArray);
			})
		})
	})
	.catch(error => response.sendStatus(500))
});

// Информация о конкретной аренде пользователя
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
		
		makeRequest(adress.cars+path+'/cars_by_uid', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({carsUidsArr: carsUids})
		}, 'cars')
		.then(resCars => {
			makeRequest(adress.payment+path+'/payment_by_uid', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({paymentUidsArr: paymentUids})
			}, 'payment')
			.then(resPayment => {
				let responseObj = {
					rentalUid: resData[0].rental_uid,
					status: resData[0].status,
					dateFrom: resData[0].date_from,
					dateTo: resData[0].date_to,
					car: undefined,
					payment: undefined
				}
					
				if(resCars != 500) {
					responseObj.car = {
						carUid: carsUids[0],
						brand: (resCars[carsUids[0]]).brand,
						model: (resCars[carsUids[0]]).model,
						registrationNumber: (resCars[carsUids[0]]).registration_number
					}
				} else {
					responseObj.car = carsUids[0]
				}
					
				if(resPayment != 500) {
					responseObj.payment =  {
						paymentUid: paymentUids[0],
						status: (resPayment[paymentUids[0]]).status,
						price: (resPayment[paymentUids[0]]).price
					}
				} else {
					responseObj.payment = {}
				}
				
				response.status(200).json(responseObj);
			})
		})
	})
	.catch(error => response.sendStatus(500))
});

// Забронировать автомобиль
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
				})
				.catch(rentalErr => {
					let params = {
						status: 503,
						statusText: 'Rental offline'
					}
					return new Response(null, params);
				}),
				fetch(adress.payment+path+'/payment/add', {
					method: 'POST',
					headers: {
						'Content-Type': 'application/json'
					},
					body: JSON.stringify(paymentParams)
				})
				.catch(paymentErr => {
					let params = {
						status: 503,
						statusText: 'Payment offline'
					}
					return new Response(null, params);
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
					fetch(adress.cars+path+'/free_car?'+new URLSearchParams({carUid: carsParams.carUid}), {
						method: 'PUT'
					});
					
					if (resArr[0].status == 503) {
						fetch(adress.cars+path+'/payment/add/rollback?'+new URLSearchParams({paymentUid: paymentParams.paymentUid}), {
							method: 'DELETE'
						})
						
						response.status(503).json({message: 'Rental Service unavailable'})
					} else if (resArr[1].status == 503) {
						fetch(adress.cars+path+'/rental/add/rollback?'+new URLSearchParams({rentalUid: rentalParams.rentalUid}), {
							method: 'DELETE'
						})
						
						response.status(503).json({message: 'Payment Service unavailable'})
					}
				}
			})
		}  else {
			response.status(400).json({message: 'Ошибка: Автомобиль уже забронирован'})
		}
	})
	.catch(error => response.status(503).json({message: 'Cars is unavailable'}))
});

// Отмена аренды автомобиля
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
				})
				.catch(rentalErr => {
					circuitBreakerObj.requestQueue.push(setInterval(fetch, 10000, adress.rental+path+'/change_status',
					{
						method: 'PUT',
						headers: {
							'Content-Type': 'application/json'
						},
						body: JSON.stringify(rentalPutParams)
					}))
				}),
				fetch(adress.cars+path+'/free_car?'+new URLSearchParams({carUid: resData.car_uid}), {
					method: 'PUT'
				})
				.catch(carsErr => {
					circuitBreakerObj.requestQueue.push(setInterval(fetch, 10000, adress.cars+path+'/free_car?'+new URLSearchParams({carUid: resData.car_uid}),
					{
					method: 'PUT'
					}))
				}),
				fetch(adress.payment+path+'/cancel_payment?'+new URLSearchParams({paymentUid: resData.payment_uid}), {
					method: 'PUT'
				})
				.catch(paymentErr => {
					circuitBreakerObj.requestQueue.push(setInterval(fetch, 10000, adress.payment+path+'/cancel_payment?'+new URLSearchParams({paymentUid: resData.payment_uid}),
					{
					method: 'PUT'
					}))
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

// Завершение аренды автомобиля
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

function checkHealth(path, serverName) {
	console.log(path)
	console.log(serverName)
	fetch(path + '/manage/health', {
		method: 'GET'
	})
	.then(res => {
		if (res.status == 200) {
			console.log('Server ' + serverName + ' is online');
			circuitBreakerObj[serverName] = 0;
			clearInterval(circuitBreakerObj.checkConnection[serverName])
		}
	})
	.catch(err => {
		console.log('Server ' + serverName + ' is offline');
	})
}

async function makeRequest(url, params, serverName) {
	if (circuitBreakerObj[serverName] <= circuitBreakerObj.maxAttempts) {
		const response = await fetch(url, params).catch(error => {
			let params = {
				status: 500,
				statusText: 'Server offline'
			}
			return new Response(null, params);
		});
		
		console.log(response.status)
		if (response.status != 500) {
			let result = await response.json();
			return result;
		} else {
			circuitBreakerObj[serverName] += 1;
			
			if (circuitBreakerObj[serverName] > circuitBreakerObj.maxAttempts) {
				circuitBreakerObj.checkConnection[serverName] = setInterval(checkHealth, 5000, adress[serverName], serverName);
			}
			
			return 500;
		}
	} else {
		return 500;
	}
}
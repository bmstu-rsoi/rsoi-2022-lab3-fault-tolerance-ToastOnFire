FROM node:19-alpine
WORKDIR ./
COPY ./package.json .
RUN npm cache clean --force
RUN npm install
COPY . .

EXPOSE 8050
CMD [ "node", "payment.js" ]

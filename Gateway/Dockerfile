FROM node:19-alpine
WORKDIR ./
COPY ./package.json .
RUN npm cache clean --force
RUN npm install
COPY . .

EXPOSE 8080
CMD [ "node", "gateway.js" ]

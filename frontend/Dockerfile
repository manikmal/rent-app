FROM node:20-alpine

WORKDIR /app

COPY package.json .
RUN npm install

COPY public ./public
COPY src ./src

ENV HOST=0.0.0.0
ENV PORT=3000
ENV REACT_APP_API_BASE=http://localhost:8000

EXPOSE 3000

CMD ["npm", "start"]

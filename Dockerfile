FROM golang:1.23 as builder

WORKDIR /app

COPY . .

RUN make shisui


FROM ubuntu:22.04

COPY --from=builder /app/build/bin/shisui /usr/local/bin/app

EXPOSE 8545 9009/udp

ENTRYPOINT [ "app" ]
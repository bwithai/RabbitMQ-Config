# Example Usage
ch is the channel object in RabbitMQ.

Think of the flow like this:
`Connection` → connect to RabbitMQ server
`Channel (ch)` → a communication pipe inside that connection
`Exchange` → receives messages
`Queue` → stores messages
`Consumer` → reads messages from queue

```python
with pika.BlockingConnection(params) as conn:
    ch = conn.channel()

ch.exchange_declare(...)   # create exchange
ch.queue_declare(...)      # create queue
ch.queue_bind(...)         # bind queue
ch.basic_publish(...)      # send message
ch.basic_consume(...)      # receive message
```


## check the openSSl in windows
```bash
where.exe openssl 2>$null; if (Test-Path "C:\Program Files\Git\usr\bin\openssl.exe") { & "C:\Program Files\Git\usr\bin\openssl.exe" version }; docker version 2>$null | Select-Object -First 5
```

##### Option A — Use Git’s OpenSSL (fastest).
In PowerShell, either call it by full path, or extend PATH for this session:
```bash
$env:Path += ";C:\Program Files\Git\usr\bin"
cd C:\Office\rabbit-mq\certs
openssl genrsa -out ca-key.pem 4096
openssl req -new -x509 -days 3650 -key ca-key.pem -out ca.pem -subj "/CN=Offline RabbitMQ CA"
openssl genrsa -out server.key 4096
openssl req -new -key server.key -out server.csr -subj "/CN=rabbitmq-vm" -addext "subjectAltName=IP:192.168.1.50,DNS:rabbitmq-vm,DNS:localhost"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -out server.pem -days 825 -copy_extensions copy
Remove-Item server.csr
```
Replace `192.168.1.50` with your real `RABBITMQ_BIND_IP` from `.env`


##### Option B — Install OpenSSL on PATH (permanent) 
Then open a new PowerShell and use openssl normally. download from https://slproweb.com/products/Win32OpenSSL.html


## Configuration
If you want to force re-pull the RabbitMQ image (optional):
```bash
docker compose pull
docker compose up -d
```

## Broker admin user and password (`.env`)
```bash
docker compose down -v
docker compose up -d
```

The `-v` flag removes the named volume (`rabbitmq_data`). After that, a fresh node picks up `RABBITMQ_DEFAULT_USER` / `RABBITMQ_DEFAULT_PASS` from `.env`.

### Keep existing data: create or update users manually

If the volume already existed (typical after the first `docker compose up`), edit `.env` alone **will not** create or rename users. Use `rabbitmqctl` inside the running container, for example:
```bash
docker exec rabbitmq rabbitmqctl add_user psadmin "admin12.12"
docker exec rabbitmq rabbitmqctl set_permissions -p / psadmin ".*" ".*" ".*"
docker exec rabbitmq rabbitmqctl set_user_tags psadmin administrator
```

To change an existing user’s password:
```bash
docker exec rabbitmq rabbitmqctl change_password psadmin "NEW_PASSWORD"
```

Check which users exist:
```bash
docker exec rabbitmq rabbitmqctl list_users
```
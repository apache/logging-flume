mkdir tmp
rm ../truststorefile.jks
rm ../keystorefile.jks
rm ../server.flume-keystore.p12
# Create the CA key and certificate
openssl req -config rootca.conf -new -x509 -nodes -keyout tmp/flume-cacert.key -out tmp/flume-ca.crt -days 10960
# Create the trust store and import the certificate
keytool -keystore ../truststorefile.jks -storetype jks -importcert -file 'tmp/flume-ca.crt' -keypass password -storepass password -alias flume-cacert -noprompt
#Import the root certificate
keytool -keystore ../keystorefile.jks -alias flume-ca -importcert -file tmp/flume-ca.crt -keypass password -storepass password -noprompt
# Create the client private key in the client key store
keytool -genkeypair -keyalg RSA -alias client -keystore ../keystorefile.jks -storepass password -keypass password -validity 10960 -keysize 2048 -dname "CN=client.flume, C=US"
# Create a signing request for the client                         #
keytool -keystore ../keystorefile.jks -alias client -certreq -file tmp/client.csr -keypass password -storepass password
# Sign the client certificate
openssl x509 -req -CA 'tmp/flume-ca.crt' -CAkey 'tmp/flume-cacert.key' -in tmp/client.csr -out tmp/client.crt_signed -days 10960 -CAcreateserial -passin pass:password
# Verify the signed certificate
openssl verify -CAfile 'tmp/flume-ca.crt' tmp/client.crt_signed
#Import the client's signed certificate
keytool -keystore ../keystorefile.jks -alias client -importcert -file tmp/client.crt_signed -keypass password -storepass password -noprompt
#Verify the keystore
keytool -list -v -keystore ../keystorefile.jks -storepass password
# Create the server private key in the server key store
keytool -genkeypair -keyalg RSA -alias server -keystore ../server.flume-keystore.p12 -storepass password -storetype PKCS12 -keypass password -validity 10960 -keysize 2048 -dname "CN=server.flume, C=US"
# Create a signing request for the server                         #
keytool -keystore ../server.flume-keystore.p12 -alias server -certreq -file tmp/server.csr -keypass password -storepass password
# Sign the server certificate
openssl x509 -req -CA 'tmp/flume-ca.crt' -CAkey 'tmp/flume-cacert.key' -in tmp/server.csr -out ../server.flume-crt.pem -days 10960 -CAcreateserial -passin pass:password
# Extract the private key
openssl pkcs12 -in ../server.flume-keystore.p12 -passin pass:password -nokeys -out ../server.flume.pem
rm -rf tmp

from flask import Flask, request, jsonify
from functools import wraps
import config.config  as config
from factories.ServiceFactory import ServiceFactory
from OpenSSL import SSL

app = Flask(__name__)

# Token-based authentication
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token or token != f'Bearer {config.TOKEN}':  
            return jsonify({'message': 'Token is missing or invalid!'}), 401
        return f(*args, **kwargs)
    return decorated

@app.route('/service/<service_name>', methods=['GET'])
@token_required
def use_service(service_name):
    # Retrieve parameters from the request
    params = request.args
    
    # Initialize the service using the factory
    service = ServiceFactory.getService(service_name)
    if not service:
        return jsonify({'message': f'Service {service_name} not found!'}), 404  
        
    result = service.get_data(params)

    return jsonify(result)

if __name__ == '__main__':
    app.run( 
        debug=True,
        host='0.0.0.0',  # Listen on all interfaces,
        ssl_context=('certs/klik-b2b-api.corpnet.pl.pem', 'certs/klik-b2b-api.corpnet.pl.key'),
        port=12130)
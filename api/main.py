#############################################
#
#   
#

from flask import Flask, request
from flask_restful import Resource, Api
import sys

class Ping(Resource):
    def get(self):  

    	return "Alive" 
	
if __name__ == "__main__":

	if len(sys.argv) != 1:
		print("Usage: python3 main.py")
		sys.exit(-1)

	app = Flask(__name__)
	api = Api(app)

	api.add_resource(Ping, '/')

	app.run(debug = True)
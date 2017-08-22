'''
* Copyright (c) 2017 The Hyve B.V.
* This code is licensed under the GNU General Public License,
* version 3.
* Author: Carolyn Langen
* Based on code from https://github.com/thehyve/transmart-fair-data-point
*
* For an overview of the DTL FAIR data point see https://dtl-fair.atlassian.net/wiki/display/FDP/FAIR+Data+Point+Software+Specification
*
*
'''

import falcon, json
import fair_metadata


class TurtleRdf(object):
    def __init__(self, producer):
        self.producer = producer

    def on_get(self, req, resp, param1=None):
        resp.status = falcon.HTTP_200
        resp.content_type = 'text/turtle'
        if param1 is None:
            g = self.producer()
        else:
            g = self.producer(param1)
        resp.body = g.serialize(format='turtle')


# Read project config from file
with open("testConfig.json", 'r') as infile:
    config = json.load(infile)

# Read FDP specifications from file
with open("fdp_specification.json", 'r') as infile:
    specs = json.load(infile)

metadata = fair_metadata.FairMetadata(config, specs)

metadata.catalog("MRC03")
# Add endpoints
api = falcon.API()
api.add_route('/', TurtleRdf(metadata.fdp))
api.add_route('/organizations', TurtleRdf(metadata.catalogs))
api.add_route('/organizations/{param1}', TurtleRdf(metadata.catalog))
# api.add_route('/studies/{param1}/data', TurtleRdf(metadata.distribution))

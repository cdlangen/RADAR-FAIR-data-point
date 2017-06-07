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

import falcon
from radar_fair_metadata import RadarFairMetadata


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


radar_fair_metadata = RadarFairMetadata()

# Add endpoints
api = falcon.API()
api.add_route('/', TurtleRdf(radar_fair_metadata.repository))
api.add_route('/studies', TurtleRdf(radar_fair_metadata.catalog))
api.add_route('/studies/{param1}', TurtleRdf(radar_fair_metadata.dataset))
api.add_route('/studies/{param1}/data', TurtleRdf(radar_fair_metadata.distribution))

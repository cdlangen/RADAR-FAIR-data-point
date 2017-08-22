"""
* Copyright (c) 2017 The Hyve B.V.
* This code is licensed under the GNU General Public License,
* version 3.
* Author: Carolyn Langen
*
* The FAIR data point (FDP)
"""
from rdflib import Literal, URIRef, Graph
from api_connector import ApiConnector
import logging, copy

# TODO: allow for additional user-defined metadata
# TODO: support placeholder's (e.g. "{catalog}")

class FairMetadata(object):
    def __init__(self, conf, specs):
        #TODO: verify logger setup
        #set up the logger
        self.logger = logging.getLogger("fdp")
        self.logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler('errors.log')
        fh.setLevel(logging.DEBUG)
        self.logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        self.logger.addHandler(ch)

        # initialize variables and validate the configuration
        self.config = conf
        self.specs = specs
        self.logger.debug("Validating the FDP configuration.")
        # TODO self.validate_configuration(conf, specs)

        self.logger.debug("Initializing the input API.")
        self.radar_api = ApiConnector(host=conf['api_host'], user=conf['api_user'], password=conf['api_password'])

    @staticmethod
    def validate_configuration(conf, specs):
        """
        Validate the values provided in the provided configuration to make sure that they conform to the specifications.
        Throws errors when specifications are violated.

        :param conf: The configuration
        :param specs: The specifications
        :return:
        """
        # make sure that the required fields are required are in the configuration
        raise NotImplementedError()

    def get_typed_variable(self, d):
        """
        Convert the input var_value into the given var_type

        :param dictionary containing the following:
            var_type: string with value Literal, URI, API_Literal or API_URI
            var_value: string or number
            var_name (optional): in the case that var_type = API_*, this specifies the variable name corresponding to
                the appropriate variable in the response.
        :return: A Literal or URIRef of the input var_value
        """
        # TODO: validate types of each variable (Literal, URI, Date, FOAF:Agent)
        #initialize variables
        var_type = d['type']
        var_name = d['varname'] if 'varname' in d else None
        var_value = d['value']

        if var_type == 'Literal':
            return Literal(var_value)
        elif var_type == 'URI':
            return URIRef(var_value)
        elif var_type == 'API_Literal' or var_type == 'API_URI':
            (_,t) = var_type.split('_')
            reply = self.radar_api.get(url=var_value)
            # TODO: add support for nested variables
            try:
                if t == 'Literal':
                    return Literal(reply[var_name])
                else:
                    return URIRef(reply[var_name])
            except:
                raise KeyError("API response does not contain variable '" + var_name + "'")
        else:
            raise TypeError("Configuration variable must be of type Literal, URI, API_Literal or API_URI")

    @staticmethod
    def replace_placeholders(placeholder, value, d):
        """
        Recursively creates a copy of the input dictionary (d) and replaces all instances of the placeholder with value
        for all strings

        :param placeholder: a string to replace
        :param value: the replacement string
        :param d: the input dictionary
        :return: a copy of d with all occurences of placeholder replaced by value
        """
        d = copy.deepcopy(d)
        for key in d:
            if isinstance(d[key], dict):
                d[key] = FairMetadata.replace_placeholders(placeholder, value, d[key])
            elif isinstance(d[key], list):
                d[key] = [li.replace(placeholder, value) for li in d[key]]
            else:
                d[key] = d[key].replace(placeholder, value) if isinstance(d[key], str) else d[key]
        return d

    def rdf_from_specs(self, config, specs, namespaces, var_dict=None):
        """
        Construct an RDF given an configuration and the relevant FDP specifications.

        :param config: Configuration for a given part of the FDP (e.g. root FDP, catalog, dataset, distribution, data
            content)
        :param specs: FDP specifications corresponding to config
        :param namespaces: A list of namespaces included in this RDF
        :param uid (optional): When the input
        :return: RDF with metadata
        """
        # replace all occurrences of the variable placeholders
        if var_dict is not None:
            for placeholder in var_dict:
                config = self.replace_placeholders(placeholder, var_dict[placeholder], config)

        # TODO: why should this be blank?
        repository = URIRef('')

        # get empty rdf graph with namespaces
        metadata = Graph()

        # add repository identifier
        id_varname = specs["identifier"]['variable_name'] if 'variable_name' in specs["identifier"] else "identifier"
        repository_id = self.get_typed_variable(config[id_varname])
        repository_id_uri = URIRef('/' + repository_id)
        metadata.add((repository_id_uri, URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), URIRef('http://purl.org/spar/datacite/ResourceIdentifier')))
        metadata.add((repository_id_uri, URIRef(specs["identifier"]['uri']), Literal(repository_id)))
        metadata.add((repository_id, URIRef(specs["identifier"]['uri']), repository_id_uri))

        #load according to provided specification
        for spec_id in specs:
            if spec_id != id_varname:
                try:
                    varname = specs[spec_id]['variable_name'] if 'variable_name' in specs[spec_id] else spec_id
                    obj = self.get_typed_variable(config[varname])
                    # TODO: should type be enforced in the specs?
                    # obj = URIRef(config[varname]) if ('is_uri' in specs[spec_id] and specs[spec_id]['is_uri']) else Literal(config[varname])
                    print((repository, specs[spec_id]['uri'], obj, varname))
                    metadata.add((repository, URIRef(specs[spec_id]['uri']), obj))
                except KeyError:
                    if specs[spec_id]['required']:
                        self.logger.exception("A required data field was not provided: " + spec_id)
                    else:
                        self.logger.info("An optional data field was not provided: " + spec_id)

        for name in namespaces:
            metadata.bind(name, namespaces[name])

        return metadata

    def get_rdf(self, config, specs, uid, placeholder):
        if uid is None:
            return self.rdf_from_specs(config,
                                       specs,
                                       self.specs['namespaces'])
        elif uid not in config:
            return self.rdf_from_specs(config[placeholder],
                                       specs,
                                       self.specs['namespaces'],
                                       {placeholder: uid})
        else:
            return self.rdf_from_specs(config[uid],
                                       specs,
                                       self.specs['namespaces'])


    def fdp(self):
        return self.get_rdf(self.config, self.specs['fdp'], None, None)

    def catalogs(self):
        """
        Iterates over all catalogs and returns a concatenation of their meta data
        :return:
        """
        metadata = Graph()
        for uid in self.config['catalogs']:
            # TODO: deal with catalogs from the API
            if uid != "{catalog}":
                metadata = metadata + self.get_rdf(self.config['catalogs'], self.specs['catalog'], uid, "{uid}")
        return metadata

    def catalog(self, uid):
        return self.get_rdf(self.config['catalogs'], self.specs['catalog'], uid, "{uid}")

    def dataset(self, uid):
        return self.get_rdf(self.config['catalogs'][uid]['dataset'], self.specs['dataset'], uid, "{uid}")


    def distribution(self, uid):
        return self.get_rdfs(self.config['catalogs'][uid]['distribution'], self.specs['distribution'], uid, "{uid}")

    def record(self, study_id):
        """Generates the metadata related to a data record."""
        raise NotImplementedError()

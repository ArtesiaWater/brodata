from . import bro, util


class SiteAssessmentData(bro.FileOrUrl):
    _rest_url = "https://publiek.broservices.nl/sq/sad/v1"
    _xmlns = "http://www.broservices.nl/xsd/dssad-internal/1.1"
    _namespace = {
        "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
        "gml": "http://www.opengis.net/gml/3.2",
        "sadcommon": "http://www.broservices.nl/xsd/sadcommon-internal/1.1",
        "xmlns": _xmlns,
    }

    def _read_contents(self, tree):
        ns = self._namespace
        sads = tree.findall(".//xmlns:SAD_O", ns)
        if len(sads) != 1:
            raise (Exception("Only one SAD_O supported"))
        sad = sads[0]
        for key in sad.attrib:
            setattr(self, key.split("}", 1)[1], sad.attrib[key])
        for child in sad:
            key = util._get_key_from_tag(child)
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "geometry":
                setattr(self, key, self._read_geometry(child))
            elif key in ["registrationHistory"]:
                self._read_children_of_children(child)
            elif key == "standardizedLocation":
                self._read_standardized_location(child)
            else:
                util._warn_unknown_key(key, self)

import pandas as pd
from functools import partial
from . import bro


class SiteAssessmentData(bro.FileOrUrl):
    """Class to represent a Site Assessment Data (SAD) from the BRO."""

    _rest_url = "https://publiek.broservices.nl/sq/sad/v1"
    _xmlns = "http://www.broservices.nl/xsd/dssad-internal/1.1"
    _char = "SAD_C"
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
            key = self._get_tag(child)
            if len(child) == 0:
                setattr(self, key, child.text)
            elif key == "geometry":
                setattr(self, key, self._read_geometry(child))
            elif key in ["registrationHistory"]:
                self._read_children_of_children(child)
            elif key == "standardizedLocation":
                self._read_standardized_location(child)
            elif key == "report":
                if hasattr(self, key):
                    self._raise_assumed_single()
                self.report = {}
                self._read_children_of_children(child, d=self.report)
            elif key == "measurementPoint":
                if not hasattr(self, key):
                    self.measurementPoint = []
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "MeasurementPoint":
                        mp = self._read_measurement_point(grandchild)
                        self.measurementPoint.append(mp)
                    else:
                        self.warn_unknown_tag(key)
            elif key == "mixedSampleAnalysis":
                if not hasattr(self, key):
                    self.mixedSampleAnalysis = []
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "MixedSampleAnalysis":
                        msa = self._read_mixed_sample_analysis(grandchild)
                        self.mixedSampleAnalysis.append(msa)
                    else:
                        self.warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)

        if hasattr(self, "measurementPoint"):
            self.measurementPoint = pd.DataFrame(self.measurementPoint)
        if hasattr(self, "mixedSampleAnalysis"):
            self.mixedSampleAnalysis = pd.DataFrame(self.mixedSampleAnalysis)

    def _read_mixed_sample_analysis(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["identification", "name", "beginDepth", "endDepth"]:
                d[key] = self._parse_text(child, key)
            elif key == "analysis":
                for grandchild in child:
                    key2 = self._get_tag(grandchild)
                    if key2 == "Analysis":
                        if key not in d:
                            d[key] = []
                        d[key].append(self._read_analysis(grandchild))
                    else:
                        self._warn_unknown_tag(key)
            elif key == "soilSampling":
                if key not in d:
                    d[key] = []
                if len(child) == 0:
                    ss = {}
                    for attrib in child.attrib:
                        key2 = attrib.split("}", 1)[1]
                        ss[key2] = child.attrib[attrib]
                    d[key].append(ss)
                else:
                    for grandchild in child:
                        key2 = self._get_tag(grandchild)
                        if key2 == "SoilSampling":
                            d[key].append(self._read_soil_sampling(grandchild))
                        else:
                            self._warn_unknown_tag(key)

            else:
                self._warn_unknown_tag(key)
        if "analysis" in d:
            d["analysis"] = pd.DataFrame(d["analysis"])
        if "soilSampling" in d:
            d["soilSampling"] = pd.DataFrame(d["soilSampling"])
        return d

    def _read_measurement_point(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["identification", "name", "date", "finalDepth", "type"]:
                d[key] = self._parse_text(child, key)
            elif key == "deliveredLocation":
                d[key] = self._read_geometry(child)
            elif key == "deliveredVerticalPosition":
                self._read_delivered_vertical_position(child, d=d)
            elif key == "boreholeSampleDescription":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "BoreholeSampleDescription":
                        self._read_borehole_sample_description(grandchild, d)
                    else:
                        self._warn_unknown_tag(key)
            elif key == "soilSampling":
                if key not in d:
                    d[key] = []
                for grandchild in child:
                    key2 = self._get_tag(grandchild)
                    if key2 == "SoilSampling":
                        d[key].append(self._read_soil_sampling(grandchild))
                    else:
                        self._warn_unknown_tag(key2)
            elif key == "filter":
                if key not in d:
                    d[key] = []
                for grandchild in child:
                    key2 = self._get_tag(grandchild)
                    if key2 == "Filter":
                        d[key].append(self._read_filter(grandchild))
                    else:
                        self._warn_unknown_tag(key2)
            else:
                self._warn_unknown_tag(key)

        if "soilSampling" in d:
            d["soilSampling"] = pd.DataFrame(d["soilSampling"])

        if "filter" in d:
            d["filter"] = pd.DataFrame(d["filter"])

        return d

    def _read_filter(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["identification", "name", "upperBoundary", "lowerBoundary"]:
                d[key] = self._parse_text(child, key)
            elif key == "deliveredVerticalPosition":
                self._read_delivered_vertical_position(child, d=d)
            elif key == "groundwaterSampling":
                for grandchild in child:
                    if key not in d:
                        d[key] = []
                    key2 = self._get_tag(grandchild)
                    if key2 == "GroundwaterSampling":
                        gs = self._read_groundwater_sampling(grandchild)
                        d[key].append(gs)
                    else:
                        self._warn_unknown_tag(key2)
            else:
                self._warn_unknown_tag(key)
        if "groundwaterSampling" in d:
            d["groundwaterSampling"] = pd.DataFrame(d["groundwaterSampling"])
        return d

    def _read_groundwater_sampling(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["identification", "name", "date"]:
                d[key] = self._parse_text(child, key)
            elif key == "groundwaterSampleAnalysis":
                for grandchild in child:
                    if key not in d:
                        d[key] = []
                    key2 = self._get_tag(grandchild)
                    if key2 == "GroundwaterSampleAnalysis":
                        gsa = self._read_groundwater_sample_analysis(grandchild)
                        d[key].append(gsa)
                    else:
                        self._warn_unknown_tag(key2)
            else:
                self._warn_unknown_tag(key)
        if "groundwaterSampleAnalysis" in d:
            d["groundwaterSampleAnalysis"] = pd.DataFrame(
                d["groundwaterSampleAnalysis"]
            )
        return d

    def _read_groundwater_sample_analysis(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["identification", "name"]:
                d[key] = self._parse_text(child, key)
            elif key == "analysis":
                for grandchild in child:
                    key2 = self._get_tag(grandchild)
                    if key2 == "Analysis":
                        if key not in d:
                            d[key] = []
                        d[key].append(self._read_analysis(grandchild))
                    else:
                        self._warn_unknown_tag(key2)
            else:
                self._warn_unknown_tag(key)
        if "analysis" in d:
            d["analysis"] = pd.DataFrame(d["analysis"])
        return d

    def _read_borehole_sample_description(self, node, d):
        for child in node:
            key = self._get_tag(child)
            if key == "descriptiveBoreholeLog":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "DescriptiveBoreholeLog":
                        if key in d:
                            self.raise_assumed_single(key)
                        d[key] = self._read_descriptive_borehole_log(grandchild)
                    else:
                        self._warn_unknown_tag(key)
            elif key == "descriptionProcedure":
                d[key] = child.text
            else:
                self._warn_unknown_tag(key)

    def _read_soil_sampling(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in ["identification", "name", "beginDepth", "endDepth", "date"]:
                d[key] = self._parse_text(child, key)
            elif key == "soilSampleAnalysis":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "SoilSampleAnalysis":
                        self._read_soil_sample_analysis(grandchild, d)
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)

    def _read_soil_sample_analysis(self, node, d):
        for child in node:
            key = self._get_tag(child)
            if key in ["identification", "name"]:
                d[key] = self._parse_text(child, key)
            elif key == "analysis":
                for grandchild in child:
                    key = self._get_tag(grandchild)
                    if key == "Analysis":
                        if "analysis" not in d:
                            d["analysis"] = []
                        d["analysis"].append(self._read_analysis(grandchild))
                    else:
                        self._warn_unknown_tag(key)
            else:
                self._warn_unknown_tag(key)
        if "analysis" in d:
            d["analysis"] = pd.DataFrame(d["analysis"])

    def _read_analysis(self, node):
        d = {}
        for child in node:
            key = self._get_tag(child)
            if key in [
                "identification",
                "quantity",
                "parameter",
                "analysisMeasurementValue",
                "condition",
                "limitSymbol",
            ]:
                to_float = ["analysisMeasurementValue"]
                d[key] = self._parse_text(child, key, to_float=to_float)
            else:
                self._warn_unknown_tag(key)
        return d


cl = SiteAssessmentData

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__

get_characteristics = partial(bro._get_characteristics, cl)
get_characteristics.__doc__ = bro._get_characteristics.__doc__

get_data_in_extent = partial(bro._get_data_in_extent, cl)
get_data_in_extent.__doc__ = bro._get_data_in_extent.__doc__

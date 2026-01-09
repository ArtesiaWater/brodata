import logging
from functools import partial

import pandas as pd

from . import bro

logger = logging.getLogger(__name__)


class ExplorationProductionConstruction(bro.FileOrUrl):
    """Class to represent an Exploration Production Construction (EPC) from the BRO."""

    _rest_url = "https://publiek.broservices.nl/ep/epc/v1"
    _xmlns = "http://www.broservices.nl/xsd/dsepc/1.0"
    _char = "EPC_C"

    def _read_contents(self, tree):
        ns = {
            "brocom": "http://www.broservices.nl/xsd/brocommon/3.0",
            "gml": "http://www.opengis.net/gml/3.2",
            "epccommon": "http://www.broservices.nl/xsd/epccommon/1.0",
            "xmlns": self._xmlns,
        }

        # Try to find EPC_PO_Borehole or EPC_PO_DP_Borehole
        epcs = tree.findall(".//xmlns:EPC_PO_Borehole", ns)
        if not epcs:
            epcs = tree.findall(".//xmlns:EPC_PO_DP_Borehole", ns)
        if not epcs:
            epcs = tree.findall(".//xmlns:EPC_PPO_Borehole", ns)

        if len(epcs) != 1:
            raise Exception(f"Expected 1 EPC object, found {len(epcs)}")

        epc = epcs[0]

        # Parse attributes
        for key in epc.attrib:
            setattr(self, key.split("}", 1)[1], epc.attrib[key])

        # Parse child elements
        for child in epc:
            key = self._get_tag(child)

            if len(child) == 0:
                # Leaf elements (text content)
                setattr(self, key, child.text)
            elif key == "standardizedLocation":
                self._read_standardized_location(child)
            elif key == "deliveredVerticalPosition":
                self._read_delivered_vertical_position(child)
            elif key == "location":
                setattr(self, key, self._read_geometry(child))
            elif key == "owner":
                self._read_owner(child)
            elif key == "sourceReference":
                self._read_source_reference(child)
            elif key in ["registrationHistory", "reportHistory"]:
                self._read_children_of_children(child)
            elif key == "lifespan":
                self._read_lifespan(child)
            elif key == "constructionHistory":
                self._read_construction_history(child)
            elif key == "horizontalPositioning":
                self._read_horizontal_positioning(child)
            elif key == "boreholeSegment":
                if self._check_single_child_with_tag(child, "BoreholeSegment"):
                    child = child[0]
                self._read_borehole_segment(child)
            elif key == "explorationProductionLicence":
                self._read_exploration_production_licence(child)
            elif key == "deliveryContext":
                setattr(self, key, child.text)
            elif key == "legalStatus":
                setattr(self, key, child.text)
            else:
                self._warn_unknown_tag(key)

        if hasattr(self, "boreholeSegment") and self.boreholeSegment:
            setattr(self, "boreholeSegment", pd.DataFrame(self.boreholeSegment))

    def _read_owner(self, node):
        """Parse owner element with optional chamber of commerce or registration number."""
        for child in node:
            key = self._get_tag(child)
            if key in ["chamberOfCommerceNumber", "europeanCompanyRegistrationNumber"]:
                setattr(self, f"owner_{key}", child.text)

    def _read_source_reference(self, node):
        """Parse sourceReference element."""
        for child in node:
            key = self._get_tag(child)
            if key in ["chamberOfCommerceNumber", "europeanCompanyRegistrationNumber"]:
                setattr(self, f"sourceReference_{key}", child.text)

    def _read_construction_history(self, node):
        """Parse constructionHistory element with multiple events."""
        events = []
        for child in node:
            key = self._get_tag(child)
            if key == "event":
                event = {}
                for grandchild in child:
                    key2 = self._get_tag(grandchild)
                    if key2 == "date":
                        event["date"] = self._read_date(grandchild)
                    elif key2 == "name":
                        event["name"] = grandchild.text
                    elif key2 == "identifier":
                        event["identifier"] = grandchild.text
                    else:
                        self._warn_unknown_tag(key2)
                events.append(event)

        if events:
            setattr(self, "constructionHistory", pd.DataFrame(events))

    def _read_horizontal_positioning(self, node):
        """Parse horizontalPositioning element."""
        for child in node:
            key = self._get_tag(child)
            if key in ["horizontalPositioningDate"]:
                setattr(self, key, self._read_date(child))
            elif key == "horizontalPositioningMethod":
                setattr(self, key, child.text)
            elif key == "horizontalPositioningOperator":
                self._read_operator(child)
            else:
                self._warn_unknown_tag(key)

    def _read_borehole_segment(self, node):
        """Parse boreholeSegment element with BoreholeSegment wrapper."""
        if not hasattr(self, "boreholeSegment"):
            self.boreholeSegment = []
        segment = {}
        for child in node:
            key = self._get_tag(child)
            if key in [
                "boreholeSegmentCode",
                "boreholeSegmentName",
                "boreholeSegmentCategory",
                "purpose",
                "drillingStartDate",
                "drillingEndDate",
                "dateOfDisclosure",
                "boreholeSegmentLocation",
            ]:
                segment[key] = child.text
            else:
                self._warn_unknown_tag(key)
        self.boreholeSegment.append(segment)

    def _read_exploration_production_licence(self, node):
        """Parse explorationProductionLicence element."""
        for child in node:
            key = self._get_tag(child)
            if key == "ExplorationProductionLicence":
                for grandchild in child:
                    key2 = self._get_tag(grandchild)
                    if key2 == "broId":
                        setattr(
                            self, "explorationProductionLicence_broId", grandchild.text
                        )
                    else:
                        self._warn_unknown_tag(key2)


cl = ExplorationProductionConstruction

get_bro_ids_of_bronhouder = partial(bro._get_bro_ids_of_bronhouder, cl)
get_bro_ids_of_bronhouder.__doc__ = bro._get_bro_ids_of_bronhouder.__doc__

get_data_for_bro_ids = partial(bro._get_data_for_bro_ids, cl)
get_data_for_bro_ids.__doc__ = bro._get_data_for_bro_ids.__doc__

get_characteristics = partial(bro._get_characteristics, cl)
get_characteristics.__doc__ = bro._get_characteristics.__doc__

get_data_in_extent = partial(bro._get_data_in_extent, cl)
get_data_in_extent.__doc__ = bro._get_data_in_extent.__doc__

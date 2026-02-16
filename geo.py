"""
NH geospatial data: towns, counties, location normalization, and distance calculations.
Ported from edopt-bot/app.py with improvements.
"""
from geopy.distance import geodesic
from fuzzywuzzy import process

# 250+ NH towns with (latitude, longitude)
NH_TOWNS = {
    'acworth': (43.2179, -72.2920), 'albany': (43.9578, -71.1676), 'alexandria': (43.6115, -71.7929),
    'allenstown': (43.1581, -71.4070), 'alstead': (43.1490, -72.3612), 'alton': (43.4534, -71.2176),
    'amherst': (42.8615, -71.6256), 'andover': (43.4367, -71.8234), 'antrim': (43.0309, -71.9389),
    'ashland': (43.6953, -71.6304), 'atkinson': (42.8384, -71.1470), 'auburn': (42.9965, -71.3484),
    'barnstead': (43.3339, -71.2928), 'barrington': (43.2229, -71.0470), 'bartlett': (44.0781, -71.2828),
    'bath': (44.1669, -71.9661), 'bedford': (42.9465, -71.5159), 'belmont': (43.4454, -71.4776),
    'bennington': (43.0031, -71.9345), 'benton': (44.1031, -71.9017), 'berlin': (44.4688, -71.1854),
    'bethlehem': (44.2803, -71.6876), 'boscawen': (43.3151, -71.6209), 'bow': (43.1320, -71.5492),
    'bradford': (43.2701, -71.9600), 'brentwood': (42.9787, -71.0728), 'bridgewater': (43.6384, -71.7365),
    'bristol': (43.5912, -71.7368), 'brookfield': (43.5589, -71.1262), 'brookline': (42.7348, -71.6581),
    'campton': (43.8703, -71.6365), 'canaan': (43.6476, -72.0118), 'candia': (43.0779, -71.2767),
    'canterbury': (43.3370, -71.5654), 'carroll': (44.2984, -71.5406), 'center harbor': (43.7098, -71.4620),
    'charlestown': (43.2387, -72.4243), 'chatham': (44.1645, -71.0112), 'chester': (42.9568, -71.2573),
    'chesterfield': (42.8873, -72.4704), 'chichester': (43.2492, -71.3998), 'claremont': (43.3767, -72.3468),
    'clarksville': (45.0167, -71.6265), 'colebrook': (44.8945, -71.4959), 'columbia': (44.8528, -71.5515),
    'concord': (43.2081, -71.5376), 'conway': (43.9792, -71.1203), 'cornish': (43.4648, -72.3684),
    'croydon': (43.4506, -72.1634), 'dalton': (44.4151, -71.6951), 'danbury': (43.5259, -71.8618),
    'danville': (42.9126, -71.1245), 'deerfield': (43.1460, -71.2164), 'deering': (43.0731, -71.8445),
    'derry': (42.8806, -71.3273), 'dover': (43.1979, -70.8737), 'dublin': (42.9076, -72.0626),
    'dummer': (44.6103, -71.2012), 'dunbarton': (43.1026, -71.6165), 'durham': (43.1339, -70.9264),
    'east kingston': (42.9256, -70.9431), 'easton': (44.1481, -71.7901), 'eaton': (43.9098, -71.0812),
    'effingham': (43.7612, -70.9967), 'enfield': (43.6406, -72.1479), 'epping': (43.0334, -71.0742),
    'epsom': (43.2229, -71.3320), 'errol': (44.7814, -71.1381), 'exeter': (42.9814, -70.9478),
    'farmington': (43.3898, -71.0651), 'fitzwilliam': (42.7806, -72.1418), 'francestown': (42.9873, -71.8129),
    'franconia': (44.2270, -71.7479), 'franklin': (43.4442, -71.6473), 'freedom': (43.8123, -71.0356),
    'fremont': (42.9909, -71.1426), 'gilford': (43.5476, -71.4067), 'gilmanton': (43.4242, -71.4145),
    'gilsum': (43.0484, -72.2629), 'goffstown': (43.0203, -71.6003), 'gorham': (44.3878, -71.1731),
    'goshen': (43.3012, -72.1476), 'grafton': (43.5587, -71.9437), 'grantham': (43.4895, -72.1376),
    'greenfield': (42.9506, -71.8723), 'greenland': (43.0336, -70.8433), 'greenville': (42.7673, -71.8123),
    'groton': (43.7015, -71.8356), 'hampstead': (42.8745, -71.1811), 'hampton': (42.9376, -70.8389),
    'hampton falls': (42.9162, -70.8637), 'hancock': (42.9729, -71.9837), 'hanover': (43.7022, -72.2895),
    'harrisville': (42.9451, -72.0965), "hart's location": (44.1045, -71.3473),
    'haverhill': (44.0345, -72.0637), 'hebron': (43.6937, -71.8056), 'henniker': (43.1798, -71.8223),
    'hill': (43.5242, -71.7012), 'hillsborough': (43.1137, -71.8956), 'hinsdale': (42.7862, -72.4865),
    'holderness': (43.7320, -71.5884), 'hollis': (42.7431, -71.5920), 'hooksett': (43.0967, -71.4651),
    'hopkinton': (43.1915, -71.6754), 'hudson': (42.7648, -71.4398), 'jackson': (44.1442, -71.1806),
    'jaffrey': (42.8140, -72.0231), 'jefferson': (44.4192, -71.4745), 'keene': (42.9337, -72.2781),
    'kensington': (42.9270, -70.9439), 'kingston': (42.9365, -71.0534), 'laconia': (43.5279, -71.4704),
    'lancaster': (44.4890, -71.5693), 'landaff': (44.1542, -71.8912), 'langdon': (43.1673, -72.3793),
    'lebanon': (43.6423, -72.2518), 'lee': (43.1231, -71.0115), 'lempster': (43.2384, -72.2106),
    'lincoln': (44.0456, -71.6704), 'lisbon': (44.2134, -71.9112), 'litchfield': (42.8443, -71.4798),
    'littleton': (44.3062, -71.7701), 'londonderry': (42.8651, -71.3740), 'loudon': (43.2856, -71.4673),
    'lyman': (44.2495, -71.9493), 'lyme': (43.8095, -72.1559), 'lyndeborough': (42.9076, -71.7665),
    'madbury': (43.1693, -70.9242), 'madison': (43.8992, -71.1484), 'manchester': (42.9956, -71.4548),
    'marlborough': (42.9040, -72.2079), 'marlow': (43.1159, -72.1970), 'mason': (42.7437, -71.7687),
    'meredith': (43.6576, -71.5004), 'merrimack': (42.8681, -71.4948), 'middleton': (43.4751, -71.0692),
    'milan': (44.5734, -71.1851), 'milford': (42.8353, -71.6489), 'milton': (43.4098, -70.9884),
    'monroe': (44.2603, -72.0476), 'mont vernon': (42.8945, -71.6742), 'moultonborough': (43.7387, -71.3967),
    'nashua': (42.7654, -71.4676), 'nelson': (42.9706, -72.1229),
    'new boston': (42.9762, -71.6939), 'new castle': (43.0723, -70.7162),
    'new durham': (43.4368, -71.1723), 'new hampton': (43.6059, -71.6540),
    'new ipswich': (42.7481, -71.8543), 'new london': (43.4140, -71.9851),
    'newbury': (43.3215, -72.0359), 'newfields': (43.0370, -70.9384),
    'newington': (43.1001, -70.8337), 'newmarket': (43.0829, -70.9351),
    'newport': (43.3653, -72.1734), 'newton': (42.8695, -71.0328),
    'north hampton': (42.9726, -70.8298), 'northfield': (43.4331, -71.5923),
    'northumberland': (44.5634, -71.5584), 'northwood': (43.1942, -71.1509),
    'nottingham': (43.1145, -71.0998), 'orange': (43.6545, -71.9715),
    'orford': (43.9053, -72.1370), 'ossipee': (43.6853, -71.1167),
    'pelham': (42.7345, -71.3247), 'pembroke': (43.1467, -71.4576),
    'peterborough': (42.8778, -71.9517), 'piermont': (43.9698, -72.0804),
    'pittsburg': (45.0512, -71.3914), 'pittsfield': (43.3059, -71.3242),
    'plainfield': (43.5340, -72.3515), 'plaistow': (42.8365, -71.0948),
    'plymouth': (43.7570, -71.6881), 'portsmouth': (43.0718, -70.7626),
    'randolph': (44.3753, -71.2798), 'raymond': (43.0362, -71.1834),
    'richmond': (42.7548, -72.2718), 'rindge': (42.7512, -72.0098),
    'rochester': (43.3045, -70.9756),
    'rollinsford': (43.2362, -70.8203), 'roxbury': (42.9248, -72.2092),
    'rumney': (43.8056, -71.8126), 'rye': (43.0134, -70.7709),
    'salem': (42.7884, -71.2009), 'salisbury': (43.3801, -71.7170),
    'sanbornton': (43.4892, -71.5823), 'sandown': (42.9287, -71.1870),
    'sandwich': (43.7904, -71.4112), 'seabrook': (42.8948, -70.8712),
    'sharon': (42.8131, -71.9156), 'shelburne': (44.4012, -71.0748),
    'somerville': (43.2645, -71.7145), 'south hampton': (42.8809, -70.9626),
    'springfield': (43.4951, -72.0334), 'stark': (44.6014, -71.4129),
    'stewartstown': (44.9967, -71.5081), 'stoddard': (43.0787, -72.1145),
    'strafford': (43.3270, -71.1842), 'stratford': (44.6531, -71.5556),
    'stratham': (43.0240, -70.9137), 'sugar hill': (44.2153, -71.7995),
    'sullivan': (43.0131, -72.2209), 'sunapee': (43.3876, -72.0879),
    'surry': (43.0179, -72.3212), 'sutton': (43.3631, -71.9495),
    'swanzey': (42.8698, -72.2818), 'tamworth': (43.8598, -71.2631),
    'temple': (42.8181, -71.8515), 'thornton': (43.8929, -71.6759),
    'tilton': (43.4423, -71.5892), 'troy': (42.8239, -72.1812),
    'tuftonboro': (43.6965, -71.2220), 'unity': (43.2934, -72.2604),
    'wakefield': (43.5681, -71.0301), 'walpole': (43.0795, -72.4259),
    'warner': (43.2809, -71.8165), 'warren': (43.9231, -71.8920),
    'washington': (43.1759, -72.0968), 'waterville valley': (44.0306, -71.4998),
    'weare': (43.0948, -71.7306), 'webster': (43.3292, -71.7179),
    'wentworth': (43.8698, -71.9115), 'westmoreland': (42.9620, -72.4423),
    'whitefield': (44.3731, -71.6101), 'wilmot': (43.4517, -71.9137),
    'wilton': (42.8431, -71.7351), 'winchester': (42.7734, -72.3831),
    'windham': (42.8006, -71.3042), 'windsor': (43.1356, -72.0006),
    'wolfeboro': (43.5859, -71.2076), 'woodstock': (43.9776, -71.6851),
}

NH_COUNTIES = {
    "belknap": {
        "center": (43.5200, -71.4234),
        "towns": ["alton", "barnstead", "belmont", "center harbor", "gilford", "gilmanton",
                   "laconia", "meredith", "new hampton", "sanbornton", "tilton"]
    },
    "carroll": {
        "center": (43.8740, -71.2080),
        "towns": ["albany", "bartlett", "brookfield", "chatham", "conway", "eaton", "effingham",
                   "freedom", "hart's location", "jackson", "madison", "moultonborough", "ossipee",
                   "sandwich", "tamworth", "tuftonboro", "wakefield", "wolfeboro"]
    },
    "cheshire": {
        "center": (42.9337, -72.2781),
        "towns": ["alstead", "chesterfield", "dublin", "fitzwilliam", "gilsum", "harrisville",
                   "hinsdale", "jaffrey", "keene", "marlborough", "marlow", "nelson", "richmond",
                   "rindge", "roxbury", "stoddard", "sullivan", "surry", "swanzey", "troy",
                   "walpole", "westmoreland", "winchester"]
    },
    "coos": {
        "center": (44.6890, -71.3059),
        "towns": ["berlin", "carroll", "clarksville", "colebrook", "columbia", "dalton", "dummer",
                   "errol", "gorham", "jefferson", "lancaster", "milan", "northumberland",
                   "pittsburg", "randolph", "shelburne", "stark", "stewartstown", "stratford",
                   "whitefield"]
    },
    "grafton": {
        "center": (43.9120, -71.9040),
        "towns": ["alexandria", "ashland", "bath", "benton", "bethlehem", "bridgewater", "bristol",
                   "campton", "canaan", "enfield", "franconia", "grafton", "groton", "hanover",
                   "haverhill", "hebron", "holderness", "landaff", "lebanon", "lincoln", "lisbon",
                   "littleton", "lyman", "lyme", "monroe", "orange", "orford", "piermont",
                   "plymouth", "rumney", "sugar hill", "thornton", "warren", "waterville valley",
                   "wentworth", "woodstock"]
    },
    "hillsborough": {
        "center": (42.9150, -71.7160),
        "towns": ["amherst", "antrim", "bedford", "bennington", "brookline", "deering",
                   "francestown", "goffstown", "greenfield", "greenville", "hancock", "hillsborough",
                   "hollis", "hudson", "litchfield", "lyndeborough", "manchester", "mason",
                   "merrimack", "milford", "mont vernon", "nashua", "new boston", "new ipswich",
                   "pelham", "peterborough", "sharon", "temple", "weare", "wilton", "windsor"]
    },
    "merrimack": {
        "center": (43.2380, -71.5600),
        "towns": ["allenstown", "andover", "boscawen", "bow", "bradford", "canterbury", "chichester",
                   "concord", "danbury", "dunbarton", "epsom", "franklin", "henniker", "hill",
                   "hopkinton", "loudon", "newbury", "new london", "northfield", "pembroke",
                   "pittsfield", "salisbury", "sutton", "warner", "webster", "wilmot"]
    },
    "rockingham": {
        "center": (42.9810, -71.0810),
        "towns": ["atkinson", "auburn", "brentwood", "candia", "chester", "danville", "deerfield",
                   "derry", "east kingston", "epping", "exeter", "fremont", "greenland", "hampstead",
                   "hampton", "hampton falls", "kensington", "kingston", "londonderry", "new castle",
                   "newfields", "newington", "newmarket", "newton", "north hampton", "nottingham",
                   "plaistow", "portsmouth", "raymond", "rye", "salem", "sandown", "seabrook",
                   "south hampton", "stratham", "windham"]
    },
    "strafford": {
        "center": (43.2650, -71.0290),
        "towns": ["barrington", "dover", "durham", "farmington", "lee", "madbury", "middleton",
                   "milton", "new durham", "northwood", "rochester", "rollinsford", "somerville",
                   "strafford"]
    },
    "sullivan": {
        "center": (43.3600, -72.2220),
        "towns": ["acworth", "charlestown", "claremont", "cornish", "croydon", "goshen", "grantham",
                   "langdon", "lempster", "newport", "plainfield", "springfield", "sunapee", "unity",
                   "washington"]
    }
}


def normalize_location(text: str):
    """
    Fuzzy-match a location string to a known NH town or county.
    Returns (name, coords, is_county) or (None, None, False) if no match.
    """
    if not text:
        return None, None, False

    text_lower = text.strip().lower()

    # Exact match on towns
    if text_lower in NH_TOWNS:
        return text_lower, NH_TOWNS[text_lower], False

    # Exact match on counties
    for county_name, county_data in NH_COUNTIES.items():
        if text_lower == county_name or text_lower == f"{county_name} county":
            return county_name, county_data["center"], True

    # Fuzzy match on towns (threshold 80)
    town_names = list(NH_TOWNS.keys())
    match, score = process.extractOne(text_lower, town_names)
    if score >= 80:
        return match, NH_TOWNS[match], False

    # Fuzzy match on county names
    county_names = list(NH_COUNTIES.keys())
    match, score = process.extractOne(text_lower, county_names)
    if score >= 80:
        return match, NH_COUNTIES[match]["center"], True

    return None, None, False


def get_nearby_towns(location: str, max_distance: float = 20.0):
    """
    Find NH towns within max_distance miles of the given location.
    Returns list of (town_name, coords, distance_miles).
    """
    name, coords, is_county = normalize_location(location)
    if coords is None:
        return []

    if is_county:
        # Return all towns in the county with their coords
        county_data = NH_COUNTIES.get(name, {})
        results = []
        for town in county_data.get("towns", []):
            if town in NH_TOWNS:
                dist = geodesic(coords, NH_TOWNS[town]).miles
                results.append((town, NH_TOWNS[town], dist))
        return sorted(results, key=lambda x: x[2])

    results = []
    for town, town_coords in NH_TOWNS.items():
        dist = geodesic(coords, town_coords).miles
        if dist <= max_distance:
            results.append((town, town_coords, dist))
    return sorted(results, key=lambda x: x[2])


def calculate_distance(coord1: tuple, coord2: tuple) -> float:
    """Calculate distance in miles between two (lat, lng) tuples."""
    if not coord1 or not coord2:
        return float('inf')
    return geodesic(coord1, coord2).miles

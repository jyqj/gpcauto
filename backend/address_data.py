"""美国地址库 — 真实城市/邮编 + 住宅地址生成器

架构: 真实城市数据 (城市名+邮编) × 住宅街道生成 = 无限逼真地址
免税州 (无州销售税): Alaska, Delaware, Montana, New Hampshire, Oregon
"""

import random
from typing import Optional

TAX_FREE_STATES = {"AK", "DE", "MT", "NH", "OR"}

US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}

# ═══════════════════════════════════════════════════════════════
# 真实城市 + 邮编数据库
# 格式: (city, zip) — 均为真实对应关系
# 免税州每州 25-40 个城市，其他州 10-20 个
# ═══════════════════════════════════════════════════════════════

# fmt: off
CITIES: dict[str, list[tuple[str, str]]] = {

    # ── 免税州 (重点覆盖) ──────────────────────────────────────

    "AK": [
        ("Anchorage", "99501"), ("Anchorage", "99502"), ("Anchorage", "99503"),
        ("Anchorage", "99504"), ("Anchorage", "99507"), ("Anchorage", "99508"),
        ("Anchorage", "99515"), ("Anchorage", "99516"), ("Anchorage", "99517"),
        ("Fairbanks", "99701"), ("Fairbanks", "99709"), ("Fairbanks", "99712"),
        ("Juneau", "99801"), ("Juneau", "99802"),
        ("Wasilla", "99654"), ("Palmer", "99645"),
        ("Sitka", "99835"), ("Ketchikan", "99901"),
        ("Kodiak", "99615"), ("Kenai", "99611"),
        ("Soldotna", "99669"), ("Homer", "99603"),
        ("Bethel", "99559"), ("North Pole", "99705"),
        ("Eagle River", "99577"), ("Valdez", "99686"),
    ],

    "DE": [
        ("Wilmington", "19801"), ("Wilmington", "19802"), ("Wilmington", "19803"),
        ("Wilmington", "19804"), ("Wilmington", "19805"), ("Wilmington", "19806"),
        ("Wilmington", "19807"), ("Wilmington", "19808"), ("Wilmington", "19809"),
        ("Wilmington", "19810"),
        ("Newark", "19702"), ("Newark", "19711"), ("Newark", "19713"),
        ("Dover", "19901"), ("Dover", "19904"),
        ("Bear", "19701"), ("Middletown", "19709"),
        ("Smyrna", "19977"), ("Milford", "19963"),
        ("Seaford", "19973"), ("Georgetown", "19947"),
        ("Lewes", "19958"), ("Rehoboth Beach", "19971"),
        ("New Castle", "19720"), ("Claymont", "19703"),
        ("Hockessin", "19707"), ("Pike Creek", "19711"),
        ("Elsmere", "19805"), ("Greenville", "19807"),
        ("Camden", "19934"), ("Harrington", "19952"),
        ("Laurel", "19956"), ("Millsboro", "19966"),
        ("Selbyville", "19975"), ("Bethany Beach", "19930"),
        ("Fenwick Island", "19944"), ("Townsend", "19734"),
    ],

    "MT": [
        ("Billings", "59101"), ("Billings", "59102"), ("Billings", "59105"),
        ("Billings", "59106"),
        ("Missoula", "59801"), ("Missoula", "59802"), ("Missoula", "59803"),
        ("Missoula", "59804"), ("Missoula", "59808"),
        ("Great Falls", "59401"), ("Great Falls", "59404"), ("Great Falls", "59405"),
        ("Bozeman", "59715"), ("Bozeman", "59718"),
        ("Helena", "59601"), ("Helena", "59602"),
        ("Butte", "59701"), ("Butte", "59702"),
        ("Kalispell", "59901"), ("Kalispell", "59903"),
        ("Havre", "59501"), ("Miles City", "59301"),
        ("Anaconda", "59711"), ("Belgrade", "59714"),
        ("Livingston", "59047"), ("Whitefish", "59937"),
        ("Laurel", "59044"), ("Sidney", "59270"),
        ("Glendive", "59330"), ("Lewistown", "59457"),
        ("Hamilton", "59840"), ("Polson", "59860"),
        ("Columbia Falls", "59912"), ("Dillon", "59725"),
    ],

    "NH": [
        ("Manchester", "03101"), ("Manchester", "03102"), ("Manchester", "03103"),
        ("Manchester", "03104"),
        ("Nashua", "03060"), ("Nashua", "03062"), ("Nashua", "03063"),
        ("Concord", "03301"), ("Concord", "03303"),
        ("Derry", "03038"), ("Dover", "03820"),
        ("Rochester", "03867"), ("Rochester", "03868"),
        ("Salem", "03079"), ("Merrimack", "03054"),
        ("Hudson", "03051"), ("Londonderry", "03053"),
        ("Keene", "03431"), ("Bedford", "03110"),
        ("Portsmouth", "03801"), ("Portsmouth", "03802"),
        ("Laconia", "03246"), ("Lebanon", "03766"),
        ("Claremont", "03743"), ("Somersworth", "03878"),
        ("Exeter", "03833"), ("Hampton", "03842"),
        ("Milford", "03055"), ("Durham", "03824"),
        ("Hanover", "03755"), ("Gilford", "03249"),
        ("Conway", "03818"), ("Wolfeboro", "03894"),
        ("Newington", "03801"), ("Hooksett", "03106"),
    ],

    "OR": [
        ("Portland", "97201"), ("Portland", "97202"), ("Portland", "97203"),
        ("Portland", "97204"), ("Portland", "97205"), ("Portland", "97206"),
        ("Portland", "97209"), ("Portland", "97210"), ("Portland", "97211"),
        ("Portland", "97212"), ("Portland", "97213"), ("Portland", "97214"),
        ("Portland", "97215"), ("Portland", "97217"), ("Portland", "97218"),
        ("Portland", "97219"), ("Portland", "97220"), ("Portland", "97221"),
        ("Portland", "97223"), ("Portland", "97225"), ("Portland", "97227"),
        ("Portland", "97229"), ("Portland", "97230"), ("Portland", "97232"),
        ("Portland", "97233"), ("Portland", "97236"),
        ("Salem", "97301"), ("Salem", "97302"), ("Salem", "97303"), ("Salem", "97304"),
        ("Eugene", "97401"), ("Eugene", "97402"), ("Eugene", "97403"), ("Eugene", "97404"),
        ("Bend", "97701"), ("Bend", "97702"), ("Bend", "97703"),
        ("Medford", "97501"), ("Medford", "97504"),
        ("Springfield", "97477"), ("Springfield", "97478"),
        ("Corvallis", "97330"), ("Corvallis", "97333"),
        ("Albany", "97321"), ("Albany", "97322"),
        ("Hillsboro", "97123"), ("Hillsboro", "97124"),
        ("Beaverton", "97005"), ("Beaverton", "97006"), ("Beaverton", "97007"),
        ("Gresham", "97030"), ("Gresham", "97080"),
        ("Lake Oswego", "97034"), ("Lake Oswego", "97035"),
        ("Tigard", "97223"), ("Tigard", "97224"),
        ("Tualatin", "97062"), ("Oregon City", "97045"),
        ("West Linn", "97068"), ("Milwaukie", "97222"),
        ("Keizer", "97303"), ("Woodburn", "97071"),
        ("McMinnville", "97128"), ("Newberg", "97132"),
        ("Ashland", "97520"), ("Grants Pass", "97526"),
        ("Roseburg", "97470"), ("Klamath Falls", "97601"),
        ("Redmond", "97756"), ("Prineville", "97754"),
        ("The Dalles", "97058"), ("Hood River", "97031"),
        ("Pendleton", "97801"), ("Hermiston", "97838"),
        ("Coos Bay", "97420"), ("Florence", "97439"),
        ("Canby", "97013"), ("Sherwood", "97140"),
        ("Happy Valley", "97086"), ("Clackamas", "97015"),
        ("Wilsonville", "97070"), ("Troutdale", "97060"),
    ],

    # ── 其余 45 州 ────────────────────────────────────────────

    "AL": [
        ("Birmingham", "35203"), ("Birmingham", "35205"), ("Birmingham", "35209"),
        ("Huntsville", "35801"), ("Huntsville", "35802"), ("Huntsville", "35805"),
        ("Montgomery", "36104"), ("Montgomery", "36106"), ("Montgomery", "36109"),
        ("Mobile", "36602"), ("Mobile", "36606"), ("Mobile", "36609"),
        ("Tuscaloosa", "35401"), ("Hoover", "35244"), ("Dothan", "36301"),
        ("Auburn", "36830"), ("Decatur", "35601"), ("Florence", "35630"),
    ],
    "AZ": [
        ("Phoenix", "85001"), ("Phoenix", "85003"), ("Phoenix", "85004"),
        ("Phoenix", "85006"), ("Phoenix", "85008"), ("Phoenix", "85012"),
        ("Phoenix", "85013"), ("Phoenix", "85014"), ("Phoenix", "85016"),
        ("Scottsdale", "85251"), ("Scottsdale", "85254"), ("Scottsdale", "85257"),
        ("Tucson", "85701"), ("Tucson", "85704"), ("Tucson", "85710"),
        ("Mesa", "85201"), ("Mesa", "85202"), ("Tempe", "85281"),
        ("Chandler", "85224"), ("Glendale", "85301"), ("Gilbert", "85233"),
        ("Flagstaff", "86001"), ("Sedona", "86336"), ("Prescott", "86301"),
    ],
    "AR": [
        ("Little Rock", "72201"), ("Little Rock", "72202"), ("Little Rock", "72205"),
        ("Fort Smith", "72901"), ("Fayetteville", "72701"), ("Springdale", "72762"),
        ("Jonesboro", "72401"), ("Conway", "72032"), ("Rogers", "72756"),
        ("North Little Rock", "72114"), ("Pine Bluff", "71601"),
        ("Bentonville", "72712"), ("Hot Springs", "71901"),
    ],
    "CA": [
        ("Los Angeles", "90001"), ("Los Angeles", "90012"), ("Los Angeles", "90024"),
        ("Los Angeles", "90036"), ("Los Angeles", "90045"), ("Los Angeles", "90064"),
        ("San Francisco", "94102"), ("San Francisco", "94103"), ("San Francisco", "94107"),
        ("San Francisco", "94109"), ("San Francisco", "94110"), ("San Francisco", "94114"),
        ("San Diego", "92101"), ("San Diego", "92103"), ("San Diego", "92104"),
        ("San Jose", "95110"), ("San Jose", "95112"), ("San Jose", "95125"),
        ("Sacramento", "95811"), ("Sacramento", "95814"), ("Sacramento", "95816"),
        ("Oakland", "94601"), ("Oakland", "94610"), ("Oakland", "94612"),
        ("Fresno", "93701"), ("Fresno", "93726"), ("Long Beach", "90802"),
        ("Santa Monica", "90401"), ("Pasadena", "91101"), ("Irvine", "92602"),
        ("Berkeley", "94704"), ("Palo Alto", "94301"), ("Redwood City", "94061"),
        ("Burbank", "91501"), ("Glendale", "91201"), ("Torrance", "90501"),
    ],
    "CO": [
        ("Denver", "80202"), ("Denver", "80204"), ("Denver", "80205"),
        ("Denver", "80206"), ("Denver", "80209"), ("Denver", "80210"),
        ("Denver", "80211"), ("Denver", "80218"), ("Denver", "80220"),
        ("Colorado Springs", "80903"), ("Colorado Springs", "80907"),
        ("Aurora", "80010"), ("Aurora", "80012"), ("Lakewood", "80226"),
        ("Fort Collins", "80521"), ("Boulder", "80302"), ("Pueblo", "81001"),
        ("Arvada", "80002"), ("Westminster", "80030"), ("Thornton", "80229"),
    ],
    "CT": [
        ("Hartford", "06103"), ("Hartford", "06105"), ("Hartford", "06106"),
        ("New Haven", "06510"), ("New Haven", "06511"), ("Stamford", "06901"),
        ("Bridgeport", "06604"), ("Waterbury", "06702"), ("Norwalk", "06851"),
        ("Danbury", "06810"), ("New Britain", "06051"), ("West Hartford", "06107"),
        ("Greenwich", "06830"), ("Fairfield", "06824"), ("Hamden", "06514"),
    ],
    "FL": [
        ("Miami", "33125"), ("Miami", "33130"), ("Miami", "33131"),
        ("Miami", "33139"), ("Miami", "33142"), ("Miami", "33155"),
        ("Orlando", "32801"), ("Orlando", "32803"), ("Orlando", "32806"),
        ("Tampa", "33602"), ("Tampa", "33606"), ("Tampa", "33609"),
        ("Jacksonville", "32202"), ("Jacksonville", "32204"), ("Jacksonville", "32207"),
        ("St Petersburg", "33701"), ("Fort Lauderdale", "33301"),
        ("Tallahassee", "32301"), ("Gainesville", "32601"),
        ("Sarasota", "34236"), ("Naples", "34102"), ("Pensacola", "32501"),
    ],
    "GA": [
        ("Atlanta", "30303"), ("Atlanta", "30305"), ("Atlanta", "30306"),
        ("Atlanta", "30308"), ("Atlanta", "30309"), ("Atlanta", "30312"),
        ("Atlanta", "30313"), ("Atlanta", "30318"), ("Atlanta", "30324"),
        ("Savannah", "31401"), ("Savannah", "31404"), ("Augusta", "30901"),
        ("Columbus", "31901"), ("Macon", "31201"), ("Athens", "30601"),
        ("Marietta", "30060"), ("Roswell", "30075"), ("Sandy Springs", "30328"),
    ],
    "HI": [
        ("Honolulu", "96813"), ("Honolulu", "96814"), ("Honolulu", "96815"),
        ("Honolulu", "96816"), ("Honolulu", "96817"), ("Honolulu", "96818"),
        ("Hilo", "96720"), ("Kailua", "96734"), ("Kaneohe", "96744"),
        ("Pearl City", "96782"), ("Waipahu", "96797"), ("Kapolei", "96707"),
        ("Mililani", "96789"), ("Aiea", "96701"), ("Lahaina", "96761"),
    ],
    "ID": [
        ("Boise", "83702"), ("Boise", "83704"), ("Boise", "83705"),
        ("Boise", "83706"), ("Boise", "83709"), ("Boise", "83712"),
        ("Meridian", "83642"), ("Nampa", "83651"), ("Nampa", "83686"),
        ("Idaho Falls", "83401"), ("Pocatello", "83201"), ("Caldwell", "83605"),
        ("Twin Falls", "83301"), ("Coeur d'Alene", "83814"),
        ("Moscow", "83843"), ("Lewiston", "83501"), ("Eagle", "83616"),
    ],
    "IL": [
        ("Chicago", "60601"), ("Chicago", "60602"), ("Chicago", "60605"),
        ("Chicago", "60607"), ("Chicago", "60610"), ("Chicago", "60611"),
        ("Chicago", "60613"), ("Chicago", "60614"), ("Chicago", "60616"),
        ("Chicago", "60618"), ("Chicago", "60622"), ("Chicago", "60625"),
        ("Chicago", "60626"), ("Chicago", "60640"), ("Chicago", "60647"),
        ("Springfield", "62701"), ("Peoria", "61602"), ("Rockford", "61101"),
        ("Naperville", "60540"), ("Evanston", "60201"), ("Champaign", "61820"),
        ("Aurora", "60505"), ("Joliet", "60431"), ("Elgin", "60120"),
    ],
    "IN": [
        ("Indianapolis", "46201"), ("Indianapolis", "46202"), ("Indianapolis", "46204"),
        ("Indianapolis", "46205"), ("Indianapolis", "46208"), ("Indianapolis", "46220"),
        ("Fort Wayne", "46802"), ("Fort Wayne", "46805"), ("Evansville", "47708"),
        ("South Bend", "46601"), ("Carmel", "46032"), ("Fishers", "46038"),
        ("Bloomington", "47401"), ("Lafayette", "47901"), ("Muncie", "47303"),
    ],
    "IA": [
        ("Des Moines", "50309"), ("Des Moines", "50310"), ("Des Moines", "50311"),
        ("Cedar Rapids", "52401"), ("Cedar Rapids", "52402"),
        ("Davenport", "52801"), ("Sioux City", "51101"), ("Iowa City", "52240"),
        ("Waterloo", "50701"), ("Council Bluffs", "51501"), ("Ames", "50010"),
        ("Dubuque", "52001"), ("West Des Moines", "50265"),
    ],
    "KS": [
        ("Wichita", "67202"), ("Wichita", "67203"), ("Wichita", "67206"),
        ("Overland Park", "66204"), ("Overland Park", "66212"),
        ("Kansas City", "66101"), ("Kansas City", "66102"), ("Topeka", "66603"),
        ("Olathe", "66061"), ("Lawrence", "66044"), ("Manhattan", "66502"),
        ("Shawnee", "66203"), ("Lenexa", "66215"), ("Salina", "67401"),
    ],
    "KY": [
        ("Louisville", "40202"), ("Louisville", "40203"), ("Louisville", "40204"),
        ("Louisville", "40206"), ("Louisville", "40207"), ("Louisville", "40208"),
        ("Lexington", "40502"), ("Lexington", "40503"), ("Lexington", "40507"),
        ("Bowling Green", "42101"), ("Owensboro", "42301"), ("Covington", "41011"),
        ("Frankfort", "40601"), ("Richmond", "40475"), ("Florence", "41042"),
    ],
    "LA": [
        ("New Orleans", "70112"), ("New Orleans", "70113"), ("New Orleans", "70115"),
        ("New Orleans", "70116"), ("New Orleans", "70118"), ("New Orleans", "70119"),
        ("Baton Rouge", "70801"), ("Baton Rouge", "70802"), ("Baton Rouge", "70806"),
        ("Shreveport", "71101"), ("Shreveport", "71104"),
        ("Lafayette", "70501"), ("Lake Charles", "70601"),
        ("Metairie", "70001"), ("Kenner", "70062"), ("Monroe", "71201"),
    ],
    "ME": [
        ("Portland", "04101"), ("Portland", "04102"), ("Portland", "04103"),
        ("Bangor", "04401"), ("Lewiston", "04240"), ("Auburn", "04210"),
        ("South Portland", "04106"), ("Biddeford", "04005"),
        ("Augusta", "04330"), ("Scarborough", "04074"),
        ("Westbrook", "04092"), ("Saco", "04072"),
        ("Waterville", "04901"), ("Brunswick", "04011"),
    ],
    "MD": [
        ("Baltimore", "21201"), ("Baltimore", "21202"), ("Baltimore", "21205"),
        ("Baltimore", "21209"), ("Baltimore", "21211"), ("Baltimore", "21213"),
        ("Baltimore", "21217"), ("Baltimore", "21218"), ("Baltimore", "21224"),
        ("Annapolis", "21401"), ("Frederick", "21701"), ("Rockville", "20850"),
        ("Silver Spring", "20901"), ("Columbia", "21044"), ("Bethesda", "20814"),
        ("Gaithersburg", "20877"), ("Bowie", "20715"), ("Hagerstown", "21740"),
    ],
    "MA": [
        ("Boston", "02108"), ("Boston", "02109"), ("Boston", "02110"),
        ("Boston", "02111"), ("Boston", "02113"), ("Boston", "02114"),
        ("Boston", "02115"), ("Boston", "02116"), ("Boston", "02118"),
        ("Boston", "02119"), ("Boston", "02120"), ("Boston", "02127"),
        ("Cambridge", "02138"), ("Cambridge", "02139"), ("Cambridge", "02141"),
        ("Worcester", "01602"), ("Springfield", "01103"), ("Lowell", "01851"),
        ("Somerville", "02143"), ("Brookline", "02445"), ("Newton", "02458"),
        ("Quincy", "02169"), ("Brockton", "02301"), ("Salem", "01970"),
    ],
    "MI": [
        ("Detroit", "48201"), ("Detroit", "48202"), ("Detroit", "48207"),
        ("Detroit", "48208"), ("Detroit", "48209"), ("Detroit", "48213"),
        ("Grand Rapids", "49503"), ("Grand Rapids", "49504"), ("Grand Rapids", "49506"),
        ("Ann Arbor", "48103"), ("Ann Arbor", "48104"), ("Ann Arbor", "48105"),
        ("Lansing", "48912"), ("Lansing", "48933"), ("Kalamazoo", "49001"),
        ("Flint", "48502"), ("Troy", "48083"), ("Royal Oak", "48067"),
    ],
    "MN": [
        ("Minneapolis", "55401"), ("Minneapolis", "55402"), ("Minneapolis", "55403"),
        ("Minneapolis", "55404"), ("Minneapolis", "55405"), ("Minneapolis", "55407"),
        ("Minneapolis", "55408"), ("Minneapolis", "55410"), ("Minneapolis", "55411"),
        ("St Paul", "55101"), ("St Paul", "55102"), ("St Paul", "55104"),
        ("St Paul", "55105"), ("Duluth", "55802"), ("Rochester", "55901"),
        ("Bloomington", "55420"), ("Brooklyn Park", "55443"), ("Plymouth", "55441"),
        ("Edina", "55424"), ("St Cloud", "56301"),
    ],
    "MS": [
        ("Jackson", "39201"), ("Jackson", "39202"), ("Jackson", "39204"),
        ("Gulfport", "39501"), ("Southaven", "38671"), ("Hattiesburg", "39401"),
        ("Biloxi", "39530"), ("Meridian", "39301"), ("Tupelo", "38801"),
        ("Olive Branch", "38654"), ("Oxford", "38655"), ("Starkville", "39759"),
    ],
    "MO": [
        ("Kansas City", "64105"), ("Kansas City", "64106"), ("Kansas City", "64108"),
        ("Kansas City", "64110"), ("Kansas City", "64111"), ("Kansas City", "64112"),
        ("St Louis", "63101"), ("St Louis", "63103"), ("St Louis", "63104"),
        ("St Louis", "63108"), ("St Louis", "63110"), ("St Louis", "63118"),
        ("Springfield", "65802"), ("Springfield", "65806"),
        ("Columbia", "65201"), ("Independence", "64050"),
        ("Lee's Summit", "64063"), ("St Joseph", "64501"),
    ],
    "NE": [
        ("Omaha", "68102"), ("Omaha", "68104"), ("Omaha", "68105"),
        ("Omaha", "68106"), ("Omaha", "68108"), ("Omaha", "68110"),
        ("Omaha", "68114"), ("Omaha", "68124"), ("Omaha", "68132"),
        ("Lincoln", "68502"), ("Lincoln", "68503"), ("Lincoln", "68508"),
        ("Lincoln", "68510"), ("Bellevue", "68005"), ("Grand Island", "68801"),
    ],
    "NV": [
        ("Las Vegas", "89101"), ("Las Vegas", "89102"), ("Las Vegas", "89104"),
        ("Las Vegas", "89106"), ("Las Vegas", "89107"), ("Las Vegas", "89108"),
        ("Las Vegas", "89109"), ("Las Vegas", "89117"), ("Las Vegas", "89119"),
        ("Henderson", "89002"), ("Henderson", "89014"), ("Henderson", "89015"),
        ("Reno", "89501"), ("Reno", "89502"), ("Reno", "89503"),
        ("Sparks", "89431"), ("Carson City", "89701"), ("North Las Vegas", "89030"),
    ],
    "NJ": [
        ("Newark", "07102"), ("Newark", "07103"), ("Newark", "07104"),
        ("Jersey City", "07302"), ("Jersey City", "07304"), ("Jersey City", "07306"),
        ("Paterson", "07501"), ("Elizabeth", "07201"), ("Trenton", "08608"),
        ("Camden", "08102"), ("Hoboken", "07030"), ("Morristown", "07960"),
        ("Princeton", "08540"), ("New Brunswick", "08901"),
        ("Montclair", "07042"), ("Cherry Hill", "08002"),
    ],
    "NM": [
        ("Albuquerque", "87101"), ("Albuquerque", "87102"), ("Albuquerque", "87104"),
        ("Albuquerque", "87106"), ("Albuquerque", "87108"), ("Albuquerque", "87110"),
        ("Santa Fe", "87501"), ("Santa Fe", "87505"), ("Las Cruces", "88001"),
        ("Las Cruces", "88005"), ("Rio Rancho", "87124"), ("Roswell", "88201"),
        ("Farmington", "87401"), ("Los Alamos", "87544"), ("Taos", "87571"),
    ],
    "NY": [
        ("New York", "10001"), ("New York", "10002"), ("New York", "10003"),
        ("New York", "10004"), ("New York", "10005"), ("New York", "10006"),
        ("New York", "10009"), ("New York", "10010"), ("New York", "10011"),
        ("New York", "10012"), ("New York", "10013"), ("New York", "10014"),
        ("New York", "10016"), ("New York", "10017"), ("New York", "10019"),
        ("New York", "10021"), ("New York", "10022"), ("New York", "10023"),
        ("New York", "10024"), ("New York", "10025"), ("New York", "10027"),
        ("New York", "10028"), ("New York", "10029"), ("New York", "10030"),
        ("New York", "10031"), ("New York", "10032"), ("New York", "10033"),
        ("New York", "10034"), ("New York", "10036"), ("New York", "10037"),
        ("Brooklyn", "11201"), ("Brooklyn", "11205"), ("Brooklyn", "11206"),
        ("Brooklyn", "11211"), ("Brooklyn", "11215"), ("Brooklyn", "11217"),
        ("Brooklyn", "11225"), ("Brooklyn", "11226"), ("Brooklyn", "11230"),
        ("Buffalo", "14201"), ("Buffalo", "14204"), ("Buffalo", "14207"),
        ("Rochester", "14604"), ("Rochester", "14607"), ("Syracuse", "13202"),
        ("Albany", "12203"), ("Albany", "12206"), ("Yonkers", "10701"),
        ("White Plains", "10601"), ("Ithaca", "14850"),
    ],
    "NC": [
        ("Charlotte", "28202"), ("Charlotte", "28203"), ("Charlotte", "28204"),
        ("Charlotte", "28205"), ("Charlotte", "28207"), ("Charlotte", "28209"),
        ("Raleigh", "27601"), ("Raleigh", "27603"), ("Raleigh", "27604"),
        ("Raleigh", "27607"), ("Durham", "27701"), ("Durham", "27707"),
        ("Greensboro", "27401"), ("Winston-Salem", "27101"),
        ("Fayetteville", "28301"), ("Asheville", "28801"), ("Wilmington", "28401"),
        ("Cary", "27511"), ("Chapel Hill", "27514"),
    ],
    "ND": [
        ("Fargo", "58102"), ("Fargo", "58103"), ("Fargo", "58104"),
        ("Bismarck", "58501"), ("Bismarck", "58503"), ("Bismarck", "58504"),
        ("Grand Forks", "58201"), ("Grand Forks", "58203"),
        ("Minot", "58701"), ("West Fargo", "58078"), ("Williston", "58801"),
        ("Mandan", "58554"), ("Dickinson", "58601"),
    ],
    "OH": [
        ("Columbus", "43201"), ("Columbus", "43202"), ("Columbus", "43205"),
        ("Columbus", "43206"), ("Columbus", "43210"), ("Columbus", "43215"),
        ("Cleveland", "44102"), ("Cleveland", "44103"), ("Cleveland", "44106"),
        ("Cleveland", "44113"), ("Cleveland", "44114"),
        ("Cincinnati", "45202"), ("Cincinnati", "45206"), ("Cincinnati", "45208"),
        ("Dayton", "45402"), ("Toledo", "43604"), ("Akron", "44302"),
        ("Canton", "44702"), ("Youngstown", "44502"),
    ],
    "OK": [
        ("Oklahoma City", "73102"), ("Oklahoma City", "73104"),
        ("Oklahoma City", "73106"), ("Oklahoma City", "73108"),
        ("Oklahoma City", "73112"), ("Oklahoma City", "73118"),
        ("Tulsa", "74103"), ("Tulsa", "74104"), ("Tulsa", "74105"),
        ("Tulsa", "74106"), ("Tulsa", "74114"), ("Tulsa", "74120"),
        ("Norman", "73069"), ("Edmond", "73003"), ("Broken Arrow", "74011"),
        ("Lawton", "73501"), ("Stillwater", "74074"),
    ],
    "PA": [
        ("Philadelphia", "19102"), ("Philadelphia", "19103"), ("Philadelphia", "19104"),
        ("Philadelphia", "19106"), ("Philadelphia", "19107"), ("Philadelphia", "19109"),
        ("Philadelphia", "19111"), ("Philadelphia", "19118"), ("Philadelphia", "19120"),
        ("Philadelphia", "19121"), ("Philadelphia", "19122"), ("Philadelphia", "19123"),
        ("Philadelphia", "19125"), ("Philadelphia", "19130"), ("Philadelphia", "19143"),
        ("Pittsburgh", "15201"), ("Pittsburgh", "15203"), ("Pittsburgh", "15206"),
        ("Pittsburgh", "15213"), ("Pittsburgh", "15217"), ("Pittsburgh", "15219"),
        ("Harrisburg", "17101"), ("Allentown", "18101"), ("Erie", "16501"),
        ("Reading", "19601"), ("Lancaster", "17601"), ("Bethlehem", "18015"),
    ],
    "RI": [
        ("Providence", "02903"), ("Providence", "02904"), ("Providence", "02906"),
        ("Providence", "02907"), ("Providence", "02908"), ("Providence", "02909"),
        ("Warwick", "02886"), ("Warwick", "02888"), ("Cranston", "02910"),
        ("Cranston", "02920"), ("Pawtucket", "02860"), ("East Providence", "02914"),
        ("Newport", "02840"), ("Woonsocket", "02895"),
    ],
    "SC": [
        ("Charleston", "29401"), ("Charleston", "29403"), ("Charleston", "29407"),
        ("Columbia", "29201"), ("Columbia", "29204"), ("Columbia", "29205"),
        ("Greenville", "29601"), ("Greenville", "29605"),
        ("Myrtle Beach", "29577"), ("North Charleston", "29405"),
        ("Rock Hill", "29730"), ("Spartanburg", "29301"),
        ("Mount Pleasant", "29464"), ("Hilton Head Island", "29928"),
    ],
    "SD": [
        ("Sioux Falls", "57103"), ("Sioux Falls", "57104"), ("Sioux Falls", "57105"),
        ("Sioux Falls", "57106"), ("Sioux Falls", "57108"),
        ("Rapid City", "57701"), ("Rapid City", "57702"),
        ("Aberdeen", "57401"), ("Brookings", "57006"), ("Watertown", "57201"),
        ("Mitchell", "57301"), ("Pierre", "57501"), ("Yankton", "57078"),
    ],
    "TN": [
        ("Nashville", "37201"), ("Nashville", "37203"), ("Nashville", "37204"),
        ("Nashville", "37206"), ("Nashville", "37208"), ("Nashville", "37209"),
        ("Nashville", "37210"), ("Nashville", "37212"), ("Nashville", "37215"),
        ("Memphis", "38103"), ("Memphis", "38104"), ("Memphis", "38106"),
        ("Memphis", "38111"), ("Memphis", "38117"), ("Memphis", "38122"),
        ("Knoxville", "37902"), ("Knoxville", "37916"), ("Chattanooga", "37402"),
        ("Clarksville", "37040"), ("Murfreesboro", "37129"), ("Franklin", "37064"),
    ],
    "TX": [
        ("Houston", "77002"), ("Houston", "77003"), ("Houston", "77004"),
        ("Houston", "77006"), ("Houston", "77007"), ("Houston", "77008"),
        ("Houston", "77009"), ("Houston", "77019"), ("Houston", "77024"),
        ("Dallas", "75201"), ("Dallas", "75204"), ("Dallas", "75206"),
        ("Dallas", "75207"), ("Dallas", "75208"), ("Dallas", "75214"),
        ("Austin", "78701"), ("Austin", "78702"), ("Austin", "78703"),
        ("Austin", "78704"), ("Austin", "78705"),
        ("San Antonio", "78201"), ("San Antonio", "78204"), ("San Antonio", "78207"),
        ("San Antonio", "78209"), ("San Antonio", "78212"),
        ("Fort Worth", "76102"), ("Fort Worth", "76104"), ("Fort Worth", "76107"),
        ("El Paso", "79901"), ("El Paso", "79902"),
        ("Plano", "75023"), ("Arlington", "76010"),
    ],
    "UT": [
        ("Salt Lake City", "84101"), ("Salt Lake City", "84102"),
        ("Salt Lake City", "84103"), ("Salt Lake City", "84104"),
        ("Salt Lake City", "84105"), ("Salt Lake City", "84106"),
        ("Salt Lake City", "84108"), ("Salt Lake City", "84111"),
        ("Provo", "84601"), ("Provo", "84604"), ("Ogden", "84401"),
        ("Sandy", "84070"), ("Orem", "84057"), ("West Jordan", "84084"),
        ("Layton", "84041"), ("St George", "84770"), ("Park City", "84060"),
    ],
    "VT": [
        ("Burlington", "05401"), ("Burlington", "05403"), ("Burlington", "05408"),
        ("South Burlington", "05403"), ("Rutland", "05701"), ("Montpelier", "05602"),
        ("Barre", "05641"), ("Bennington", "05201"), ("Brattleboro", "05301"),
        ("St Albans", "05478"), ("Middlebury", "05753"), ("Essex Junction", "05452"),
        ("Hartford", "05047"), ("Winooski", "05404"),
    ],
    "VA": [
        ("Richmond", "23219"), ("Richmond", "23220"), ("Richmond", "23221"),
        ("Richmond", "23222"), ("Richmond", "23223"), ("Richmond", "23224"),
        ("Virginia Beach", "23451"), ("Virginia Beach", "23452"),
        ("Norfolk", "23510"), ("Norfolk", "23517"), ("Arlington", "22201"),
        ("Arlington", "22202"), ("Alexandria", "22301"), ("Alexandria", "22314"),
        ("Roanoke", "24011"), ("Charlottesville", "22901"),
        ("Newport News", "23601"), ("Hampton", "23666"),
        ("McLean", "22101"), ("Fairfax", "22030"),
    ],
    "WA": [
        ("Seattle", "98101"), ("Seattle", "98102"), ("Seattle", "98103"),
        ("Seattle", "98104"), ("Seattle", "98105"), ("Seattle", "98107"),
        ("Seattle", "98109"), ("Seattle", "98112"), ("Seattle", "98115"),
        ("Seattle", "98116"), ("Seattle", "98118"), ("Seattle", "98122"),
        ("Tacoma", "98402"), ("Tacoma", "98403"), ("Tacoma", "98405"),
        ("Spokane", "99201"), ("Spokane", "99202"), ("Spokane", "99205"),
        ("Bellevue", "98004"), ("Bellevue", "98005"), ("Bellevue", "98007"),
        ("Olympia", "98501"), ("Vancouver", "98660"), ("Redmond", "98052"),
        ("Kirkland", "98033"), ("Everett", "98201"),
    ],
    "WV": [
        ("Charleston", "25301"), ("Charleston", "25302"), ("Charleston", "25304"),
        ("Huntington", "25701"), ("Huntington", "25703"),
        ("Morgantown", "26501"), ("Morgantown", "26505"),
        ("Parkersburg", "26101"), ("Wheeling", "26003"),
        ("Martinsburg", "25401"), ("Beckley", "25801"), ("Clarksburg", "26301"),
    ],
    "WI": [
        ("Milwaukee", "53202"), ("Milwaukee", "53203"), ("Milwaukee", "53204"),
        ("Milwaukee", "53206"), ("Milwaukee", "53207"), ("Milwaukee", "53208"),
        ("Milwaukee", "53210"), ("Milwaukee", "53211"), ("Milwaukee", "53212"),
        ("Madison", "53703"), ("Madison", "53704"), ("Madison", "53705"),
        ("Madison", "53711"), ("Green Bay", "54301"), ("Green Bay", "54302"),
        ("Kenosha", "53140"), ("Racine", "53402"), ("Appleton", "54911"),
        ("Oshkosh", "54901"), ("Eau Claire", "54701"), ("La Crosse", "54601"),
    ],
    "WY": [
        ("Cheyenne", "82001"), ("Cheyenne", "82003"), ("Cheyenne", "82007"),
        ("Casper", "82601"), ("Casper", "82604"), ("Laramie", "82070"),
        ("Gillette", "82716"), ("Rock Springs", "82901"), ("Sheridan", "82801"),
        ("Jackson", "83001"), ("Riverton", "82501"), ("Cody", "82414"),
    ],
}
# fmt: on

# ═══════════════════════════════════════════════════════════════
# 住宅街道生成器
# ═══════════════════════════════════════════════════════════════

_STREET_NAMES = [
    "Main", "Oak", "Maple", "Cedar", "Pine", "Elm", "Walnut", "Birch",
    "Willow", "Spruce", "Chestnut", "Hickory", "Ash", "Poplar", "Sycamore",
    "Park", "Lake", "River", "Forest", "Hill", "Valley", "Ridge", "Meadow",
    "Spring", "Sunset", "Highland", "Mountain", "Creek", "Brook", "Canyon",
    "Washington", "Jefferson", "Lincoln", "Franklin", "Adams", "Madison",
    "Jackson", "Harrison", "Grant", "Wilson", "Monroe", "Hamilton",
    "Church", "School", "Academy", "College", "Market", "Center",
    "Pleasant", "Union", "Liberty", "Prospect", "Summit", "Broad",
    "North", "South", "East", "West", "Cross", "Bridge", "Mill",
    "Vine", "Rose", "Cherry", "Peach", "Apple", "Laurel", "Magnolia",
    "Dogwood", "Holly", "Ivy", "Hazel", "Fern", "Aspen", "Juniper",
    "Colonial", "Heritage", "Patriot", "Pioneer", "Frontier", "Independence",
    "Fairview", "Greenwood", "Lakeview", "Riverside", "Westwood", "Eastwood",
    "Northwood", "Southwood", "Crestwood", "Oakwood", "Maplewood", "Cedarwood",
    "Briarwood", "Sherwood", "Glenwood", "Edgewood", "Pinewood", "Wildwood",
    "Windsor", "Cambridge", "Oxford", "Hampton", "Lancaster", "Buckingham",
    "Coventry", "Canterbury", "Westminster", "Kensington", "Stratford",
    "Woodfield", "Stonebridge", "Foxhall", "Bayberry", "Thornberry",
]

_STREET_TYPES = [
    ("St", 25), ("Ave", 20), ("Dr", 15), ("Rd", 12), ("Ln", 8),
    ("Way", 6), ("Ct", 5), ("Pl", 3), ("Blvd", 3), ("Cir", 3),
]

_DIRECTIONS = ["N", "S", "E", "W", "NE", "NW", "SE", "SW"]

_APT_FORMATS = [
    "Apt {n}", "Apt {a}", "Unit {n}", "#{n}",
    "Suite {n}", "Apt {n}{a}", "#{n}{a}",
]


def _weighted_choice(items: list[tuple[str, int]]) -> str:
    population = [x[0] for x in items]
    weights = [x[1] for x in items]
    return random.choices(population, weights=weights, k=1)[0]


def _generate_street() -> str:
    house_num = random.choices(
        [random.randint(1, 99), random.randint(100, 999),
         random.randint(1000, 4999), random.randint(5000, 19999)],
        weights=[5, 40, 40, 15], k=1
    )[0]

    name = random.choice(_STREET_NAMES)
    suffix = _weighted_choice(_STREET_TYPES)

    has_direction = random.random() < 0.15
    direction = random.choice(_DIRECTIONS) if has_direction else ""

    if direction:
        street = f"{house_num} {direction} {name} {suffix}"
    else:
        street = f"{house_num} {name} {suffix}"

    if random.random() < 0.18:
        fmt = random.choice(_APT_FORMATS)
        apt_n = random.randint(1, 412)
        apt_a = random.choice("ABCDEFG")
        apt = fmt.format(n=apt_n, a=apt_a)
        street = f"{street}, {apt}"

    return street


# ═══════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════

def get_states_list(tax_free_only: bool = False) -> list[dict]:
    """返回州列表，带免税标记和城市数量。"""
    result = []
    for code, name in sorted(US_STATES.items(), key=lambda x: x[1]):
        is_tax_free = code in TAX_FREE_STATES
        if tax_free_only and not is_tax_free:
            continue
        city_count = len(CITIES.get(code, []))
        result.append({
            "code": code,
            "name": name,
            "tax_free": is_tax_free,
            "city_count": city_count,
        })
    return result


def generate_street() -> str:
    """公开接口: 仅生成随机街道地址 (不含城市/州)。"""
    return _generate_street()


def get_random_address(
    state: Optional[str] = None,
    zip_code: Optional[str] = None,
    tax_free_only: bool = False,
) -> Optional[dict]:
    """生成一条随机但逼真的美国住宅地址。

    - state: 指定州代码 (如 "DE")
    - zip_code: 指定邮编 (精确匹配 or 前缀匹配)
    - tax_free_only: 仅从免税州
    优先级: zip_code > state > tax_free_only
    """
    pool: list[tuple[str, str, str]] = []

    if zip_code:
        zp = zip_code.strip()
        for s, cities in CITIES.items():
            if state and s != state.upper():
                continue
            if tax_free_only and s not in TAX_FREE_STATES:
                continue
            for c, z in cities:
                if z == zp or z.startswith(zp):
                    pool.append((s, c, z))
    elif state:
        pool = [(state.upper(), c, z) for c, z in CITIES.get(state.upper(), [])]
    elif tax_free_only:
        for s in TAX_FREE_STATES:
            pool.extend([(s, c, z) for c, z in CITIES.get(s, [])])
    else:
        for s, cities in CITIES.items():
            pool.extend([(s, c, z) for c, z in cities])

    if not pool:
        return None

    st, city, zipcode = random.choice(pool)
    return {
        "address_line1": _generate_street(),
        "city": city,
        "state": st,
        "zip": zipcode,
        "country": "US",
    }


def get_random_addresses(
    count: int = 1,
    state: Optional[str] = None,
    zip_code: Optional[str] = None,
    tax_free_only: bool = False,
) -> list[dict]:
    """批量生成地址。"""
    return [
        addr for _ in range(count)
        if (addr := get_random_address(state=state, zip_code=zip_code, tax_free_only=tax_free_only))
    ]

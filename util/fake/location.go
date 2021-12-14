// Copyright © 2016 SurrealDB Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fake

import (
	"fmt"
	"strings"
)

var names = []string{"Home", "Work", "Business", "Personal"}

var streets = []string{"Avenue", "Boulevard", "Center", "Circle", "Court", "Drive", "Extension", "Glen", "Grove", "Heights", "Highway", "Junction", "Key", "Lane", "Loop", "Manor", "Mill", "Park", "Parkway", "Pass", "Path", "Pike", "Place", "Plaza", "Point", "Ridge", "River", "Road", "Square", "Street", "Terrace", "Trail", "Turnpike", "View", "Way"}

var states = []string{"Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"}

var counties = []string{"Bath and North East Somerset", "Aberdeenshire", "Anglesey", "Angus", "Bedford", "Blackburn with Darwen", "Blackpool", "Bournemouth", "Bracknell Forest", "Brighton & Hove", "Bristol", "Buckinghamshire", "Cambridgeshire", "Carmarthenshire", "Central Bedfordshire", "Ceredigion", "Cheshire East", "Cheshire West and Chester", "Clackmannanshire", "Conwy", "Cornwall", "County Antrim", "County Armagh", "County Down", "County Durham", "County Fermanagh", "County Londonderry", "County Tyrone", "Cumbria", "Darlington", "Denbighshire", "Derby", "Derbyshire", "Devon", "Dorset", "Dumfries and Galloway", "Dundee", "East Lothian", "East Riding of Yorkshire", "East Sussex", "Edinburgh", "Essex", "Falkirk", "Fife", "Flintshire", "Gloucestershire", "Greater London", "Greater Manchester", "Gwent", "Gwynedd", "Halton", "Hampshire", "Hartlepool", "Herefordshire", "Hertfordshire", "Highlands", "Hull", "Isle of Wight", "Isles of Scilly", "Kent", "Lancashire", "Leicester", "Leicestershire", "Lincolnshire", "Lothian", "Luton", "Medway", "Merseyside", "Mid Glamorgan", "Middlesbrough", "Milton Keynes", "Monmouthshire", "Moray", "Norfolk", "North East Lincolnshire", "North Lincolnshire", "North Somerset", "North Yorkshire", "Northamptonshire", "Northumberland", "Nottingham", "Nottinghamshire", "Oxfordshire", "Pembrokeshire", "Perth and Kinross", "Peterborough", "Plymouth", "Poole", "Portsmouth", "Powys", "Reading", "Redcar and Cleveland", "Rutland", "Scottish Borders", "Shropshire", "Slough", "Somerset", "South Glamorgan", "South Gloucestershire", "South Yorkshire", "Southampton", "Southend-on-Sea", "Staffordshire", "Stirlingshire", "Stockton-on-Tees", "Stoke-on-Trent", "Strathclyde", "Suffolk", "Surrey", "Swindon", "Telford and Wrekin", "Thurrock", "Torbay", "Tyne and Wear", "Warrington", "Warwickshire", "West Berkshire", "West Glamorgan", "West Lothian", "West Midlands", "West Sussex", "West Yorkshire", "Western Isles", "Wiltshire", "Windsor and Maidenhead", "Wokingham", "Worcestershire", "Wrexham", "York"}

var countries = []string{"Afghanistan", "Åland Islands", "Albania", "Algeria", "American Samoa", "Andorra", "Angola", "Anguilla", "Antarctica", "Antigua & Barbuda", "Argentina", "Armenia", "Aruba", "Ascension Island", "Australia", "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bermuda", "Bhutan", "Bolivia", "Bosnia & Herzegovina", "Botswana", "Brazil", "British Indian Ocean Territory", "British Virgin Islands", "Brunei", "Bulgaria", "Burkina Faso", "Burundi", "Cambodia", "Cameroon", "Canada", "Canary Islands", "Cape Verde", "Caribbean Netherlands", "Cayman Islands", "Central African Republic", "Ceuta & Melilla", "Chad", "Chile", "China", "Christmas Island", "Cocos (Keeling) Islands", "Colombia", "Comoros", "Congo - Brazzaville", "Congo - Kinshasa", "Cook Islands", "Costa Rica", "Côte d'Ivoire", "Croatia", "Cuba", "Curaçao", "Cyprus", "Czech Republic", "Denmark", "Diego Garcia", "Djibouti", "Dominica", "Dominican Republic", "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia", "Ethiopia", "Falkland Islands", "Faroe Islands", "Fiji", "Finland", "France", "French Guiana", "French Polynesia", "French Southern Territories", "Gabon", "Gambia", "Georgia", "Germany", "Ghana", "Gibraltar", "Greece", "Greenland", "Grenada", "Guadeloupe", "Guam", "Guatemala", "Guernsey", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", "Honduras", "Hong Kong SAR China", "Hungary", "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland", "Isle of Man", "Israel", "Italy", "Jamaica", "Japan", "Jersey", "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Kosovo", "Kuwait", "Kyrgyzstan", "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg", "Macau SAR China", "Macedonia", "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands", "Martinique", "Mauritania", "Mauritius", "Mayotte", "Mexico", "Micronesia", "Moldova", "Monaco", "Mongolia", "Montenegro", "Montserrat", "Morocco", "Mozambique", "Myanmar (Burma)", "Namibia", "Nauru", "Nepal", "Netherlands", "New Caledonia", "New Zealand", "Nicaragua", "Niger", "Nigeria", "Niue", "Norfolk Island", "North Korea", "Northern Mariana Islands", "Norway", "Oman", "Pakistan", "Palau", "Palestinian Territories", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines", "Pitcairn Islands", "Poland", "Portugal", "Puerto Rico", "Qatar", "Réunion", "Romania", "Russia", "Rwanda", "Samoa", "San Marino", "São Tomé and Príncipe", "Saudi Arabia", "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Sint Maarten", "Slovakia", "Slovenia", "Solomon Islands", "Somalia", "South Africa", "South Georgia & South Sandwich Islands", "South Korea", "South Sudan", "Spain", "Sri Lanka", "St. Barthélemy", "St. Helena", "St. Kitts & Nevis", "St. Lucia", "St. Martin", "St. Pierre & Miquelon", "St. Vincent & Grenadines", "Sudan", "Suriname", "Svalbard & Jan Mayen", "Swaziland", "Sweden", "Switzerland", "Syria", "Taiwan", "Tajikistan", "Tanzania", "Thailand", "Timor-Leste", "Togo", "Tokelau", "Tonga", "Trinidad & Tobago", "Tristan da Cunha", "Tunisia", "Turkey", "Turkmenistan", "Turks & Caicos Islands", "Tuvalu", "U.S. Outlying Islands", "U.S. Virgin Islands", "Uganda", "Ukraine", "United Arab Emirates", "United Kingdom", "United States", "Uruguay", "Uzbekistan", "Vanuatu", "Vatican City", "Venezuela", "Vietnam", "Wallis & Futuna", "Western Sahara", "Yemen", "Zambia", "Zimbabwe"}

func LocationName() string {
	return New().LocationName()
}

func (f *Faker) LocationName() string {
	return names[f.r.Intn(len(names))]
}

func LocationAddress() string {
	return New().LocationAddress()
}

func (f *Faker) LocationAddress() string {
	return fmt.Sprintf("%d %s",
		f.IntegerBetween(1, 250),
		f.LocationStreet(),
	)
}

func LocationStreet() string {
	return New().LocationStreet()
}

func (f *Faker) LocationStreet() string {
	return fmt.Sprintf("%s %s",
		strings.Title(f.Word()),
		streets[f.r.Intn(len(streets))],
	)
}

func LocationCity() string {
	return New().LocationCity()
}

func (f *Faker) LocationCity() string {
	return strings.Title(f.Word())
}

func LocationState() string {
	return New().LocationState()
}

func (f *Faker) LocationState() string {
	return states[f.r.Intn(len(states))]
}

func LocationCounty() string {
	return New().LocationCounty()
}

func (f *Faker) LocationCounty() string {
	return counties[f.r.Intn(len(counties))]
}

func LocationZipcode() string {
	return New().LocationZipcode()
}

func (f *Faker) LocationZipcode() string {
	return fmt.Sprintf("%s%s %d",
		f.CharUpper(),
		f.CharUpper(),
		f.IntegerBetween(10000, 99999),
	)
}

func LocationPostcode() string {
	return New().LocationPostcode()
}

func (f *Faker) LocationPostcode() string {
	return fmt.Sprintf("%s%s%d %d%s%s",
		f.CharUpper(),
		f.CharUpper(),
		f.IntegerBetween(0, 20),
		f.IntegerBetween(0, 99),
		f.CharUpper(),
		f.CharUpper(),
	)
}

func LocationCountry() string {
	return New().LocationCountry()
}

func (f *Faker) LocationCountry() string {
	return countries[f.r.Intn(len(countries))]
}

func LocationAltitude() float64 {
	return New().LocationAltitude()
}

func (f *Faker) LocationAltitude() float64 {
	return f.r.Float64() * 8848
}

func LocationLatitude() float64 {
	return New().LocationLatitude()
}

func (f *Faker) LocationLatitude() float64 {
	return f.r.Float64()*180 - 90
}

func LocationLongitude() float64 {
	return New().LocationLongitude()
}

func (f *Faker) LocationLongitude() float64 {
	return f.r.Float64()*360 - 180
}

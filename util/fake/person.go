// Copyright © 2016 Abcum Ltd
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

var gender = []string{"Male", "Female"}

var male = []string{"James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Charles", "Thomas", "Christopher", "Daniel", "Matthew", "George", "Donald", "Anthony", "Paul", "Mark", "Edward", "Steven", "Kenneth", "Andrew", "Brian", "Joshua", "Kevin", "Ronald", "Timothy", "Jason", "Jeffrey", "Frank", "Gary", "Ryan", "Nicholas", "Eric", "Stephen", "Jacob", "Larry", "Jonathan", "Scott", "Raymond", "Justin", "Brandon", "Gregory", "Samuel", "Benjamin", "Patrick", "Jack", "Henry", "Walter", "Dennis", "Jerry", "Alexander", "Peter", "Tyler", "Douglas", "Harold", "Aaron", "Jose", "Adam", "Arthur", "Zachary", "Carl", "Nathan", "Albert", "Kyle", "Lawrence", "Joe", "Willie", "Gerald", "Roger", "Keith", "Jeremy", "Terry", "Harry", "Ralph", "Sean", "Jesse", "Roy", "Louis", "Billy", "Austin", "Bruce", "Eugene", "Christian", "Bryan", "Wayne", "Russell", "Howard", "Fred", "Ethan", "Jordan", "Philip", "Alan", "Juan", "Randy", "Vincent", "Bobby", "Dylan", "Johnny", "Phillip", "Victor", "Clarence", "Ernest", "Martin", "Craig", "Stanley", "Shawn", "Travis", "Bradley", "Leonard", "Earl", "Gabriel", "Jimmy", "Francis", "Todd", "Noah", "Danny", "Dale", "Cody", "Carlos", "Allen", "Frederick", "Logan", "Curtis", "Alex", "Joel", "Luis", "Norman", "Marvin", "Glenn", "Tony", "Nathaniel", "Rodney", "Melvin", "Alfred", "Steve", "Cameron", "Chad", "Edwin", "Caleb", "Evan", "Antonio", "Lee", "Herbert", "Jeffery", "Isaac", "Derek", "Ricky", "Marcus", "Theodore", "Elijah", "Luke", "Jesus", "Eddie", "Troy", "Mike", "Dustin", "Ray", "Adrian", "Bernard", "Leroy", "Angel", "Randall", "Wesley", "Ian", "Jared", "Mason", "Hunter", "Calvin", "Oscar", "Clifford", "Jay", "Shane", "Ronnie", "Barry", "Lucas", "Corey", "Manuel", "Leo", "Tommy", "Warren", "Jackson", "Isaiah", "Connor", "Don", "Dean", "Jon", "Julian", "Miguel", "Bill", "Lloyd", "Charlie", "Mitchell", "Leon", "Jerome", "Darrell", "Jeremiah", "Alvin", "Brett", "Seth", "Floyd", "Jim", "Blake", "Micheal", "Gordon", "Trevor", "Lewis", "Erik", "Edgar", "Vernon", "Devin", "Gavin", "Jayden", "Chris", "Clyde", "Tom", "Derrick", "Mario", "Brent", "Marc", "Herman", "Chase", "Dominic", "Ricardo", "Franklin", "Maurice", "Max", "Aiden", "Owen", "Lester", "Gilbert", "Elmer", "Gene", "Francisco", "Glen", "Cory", "Garrett", "Clayton", "Sam", "Jorge", "Chester", "Alejandro", "Jeff", "Harvey", "Milton", "Cole", "Ivan", "Andre", "Duane", "Landon"}

var female = []string{"Mary", "Emma", "Elizabeth", "Minnie", "Margaret", "Ida", "Alice", "Bertha", "Sarah", "Annie", "Clara", "Ella", "Florence", "Cora", "Martha", "Laura", "Nellie", "Grace", "Carrie", "Maude", "Mabel", "Bessie", "Jennie", "Gertrude", "Julia", "Hattie", "Edith", "Mattie", "Rose", "Catherine", "Lillian", "Ada", "Lillie", "Helen", "Jessie", "Louise", "Ethel", "Lula", "Myrtle", "Eva", "Frances", "Lena", "Lucy", "Edna", "Maggie", "Pearl", "Daisy", "Fannie", "Josephine", "Dora", "Rosa", "Katherine", "Agnes", "Marie", "Nora", "May", "Mamie", "Blanche", "Stella", "Ellen", "Nancy", "Effie", "Sallie", "Nettie", "Della", "Lizzie", "Flora", "Susie", "Maud", "Mae", "Etta", "Harriet", "Sadie", "Caroline", "Katie", "Lydia", "Elsie", "Kate", "Susan", "Mollie", "Alma", "Addie", "Georgia", "Eliza", "Lulu", "Nannie", "Lottie", "Amanda", "Belle", "Charlotte", "Rebecca", "Ruth", "Viola", "Olive", "Amelia", "Hannah", "Jane", "Virginia", "Emily", "Matilda", "Irene", "Kathryn", "Esther", "Willie", "Henrietta", "Ollie", "Amy", "Rachel", "Sara", "Estella", "Theresa", "Augusta", "Ora", "Pauline", "Josie", "Lola", "Sophia", "Leona", "Anne", "Mildred", "Ann", "Beulah", "Callie", "Lou", "Delia", "Eleanor", "Barbara", "Iva", "Louisa", "Maria", "Mayme", "Evelyn", "Estelle", "Nina", "Betty", "Marion", "Bettie", "Dorothy", "Luella", "Inez", "Lela", "Rosie", "Allie", "Millie", "Janie", "Cornelia", "Victoria", "Ruby", "Winifred", "Alta", "Celia", "Christine", "Beatrice", "Birdie", "Harriett", "Mable", "Myra", "Sophie", "Tillie", "Isabel", "Sylvia", "Carolyn", "Isabelle", "Leila", "Sally", "Ina", "Essie", "Bertie", "Nell", "Alberta", "Katharine", "Lora", "Rena", "Mina", "Rhoda", "Mathilda", "Abbie", "Eula", "Dollie", "Hettie", "Eunice", "Fanny", "Ola", "Lenora", "Adelaide", "Christina", "Lelia", "Nelle", "Sue", "Johanna", "Lilly", "Lucinda", "Minerva", "Lettie", "Roxie", "Cynthia", "Helena", "Hilda", "Hulda", "Bernice", "Genevieve", "Jean", "Cordelia", "Marian", "Francis", "Jeanette", "Adeline", "Gussie", "Leah", "Lois", "Lura", "Mittie", "Hallie", "Isabella", "Olga", "Phoebe", "Teresa", "Hester", "Lida", "Lina", "Winnie", "Claudia", "Marguerite", "Vera", "Cecelia", "Bess", "Emilie", "Rosetta", "Verna", "Myrtie", "Cecilia", "Elva", "Olivia", "Ophelia", "Georgie", "Elnora", "Violet", "Adele", "Lily", "Linnie", "Loretta", "Madge", "Polly", "Virgie", "Eugenia", "Lucile", "Lucille", "Mabelle", "Rosalie"}

var surnames = []string{"Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "Hernandez", "King", "Wright", "Lopez", "Hill", "Scott", "Green", "Adams", "Baker", "Gonzalez", "Nelson", "Carter", "Mitchell", "Perez", "Roberts", "Turner", "Phillips", "Campbell", "Parker", "Evans", "Edwards", "Collins", "Stewart", "Sanchez", "Morris", "Rogers", "Reed", "Cook", "Morgan", "Bell", "Murphy", "Bailey", "Rivera", "Cooper", "Richardson", "Cox", "Howard", "Ward", "Torres", "Peterson", "Gray", "Ramirez", "James", "Watson", "Brooks", "Kelly", "Sanders", "Price", "Bennett", "Wood", "Barnes", "Ross", "Henderson", "Coleman", "Jenkins", "Perry", "Powell", "Long", "Patterson", "Hughes", "Flores", "Washington", "Butler", "Simmons", "Foster", "Gonzales", "Bryant", "Alexander", "Russell", "Griffin", "Diaz", "Hayes", "Myers", "Ford", "Hamilton", "Graham", "Sullivan", "Wallace", "Woods", "Cole", "West", "Jordan", "Owens", "Reynolds", "Fisher", "Ellis", "Harrison", "Gibson", "McDonald", "Cruz", "Marshall", "Ortiz", "Gomez", "Murray", "Freeman", "Wells", "Webb", "Simpson", "Stevens", "Tucker", "Porter", "Hunter", "Hicks", "Crawford", "Henry", "Boyd", "Mason", "Morales", "Kennedy", "Warren", "Dixon", "Ramos", "Reyes", "Burns", "Gordon", "Shaw", "Holmes", "Rice", "Robertson", "Hunt", "Black", "Daniels", "Palmer", "Mills", "Nichols", "Grant", "Knight", "Ferguson", "Rose", "Stone", "Hawkins", "Dunn", "Perkins", "Hudson", "Spencer", "Gardner", "Stephens", "Payne", "Pierce", "Berry", "Matthews", "Arnold", "Wagner", "Willis", "Ray", "Watkins", "Olson", "Carroll", "Duncan", "Snyder", "Hart", "Cunningham", "Bradley", "Lane", "Andrews", "Ruiz", "Harper", "Fox", "Riley", "Armstrong", "Carpenter", "Weaver", "Greene", "Lawrence", "Elliott", "Chavez", "Sims", "Austin", "Peters", "Kelley", "Franklin", "Lawson", "Fields", "Gutierrez", "Ryan", "Schmidt", "Carr", "Vasquez", "Castillo", "Wheeler", "Chapman", "Oliver", "Montgomery", "Richards", "Williamson", "Johnston", "Banks", "Meyer", "Bishop", "McCoy", "Howell", "Alvarez", "Morrison", "Hansen", "Fernandez", "Garza", "Harvey", "Little", "Burton", "Stanley", "Nguyen", "George", "Jacobs", "Reid", "Kim", "Fuller", "Lynch", "Dean", "Gilbert", "Garrett", "Romero", "Welch", "Larson", "Frazier", "Burke", "Hanson", "Day", "Mendoza", "Moreno", "Bowman", "Medina", "Fowler", "Brewer", "Hoffman", "Carlson", "Silva", "Pearson", "Holland", "Douglas", "Fleming", "Jensen", "Vargas", "Byrd", "Davidson", "Hopkins", "May", "Terry", "Herrera", "Wade", "Soto", "Walters", "Curtis", "Neal", "Caldwell", "Lowe", "Jennings", "Barnett", "Graves", "Jimenez", "Horton", "Shelton", "Barrett", "Obrien", "Castro", "Sutton", "Gregory", "McKinney", "Lucas", "Miles", "Craig", "Rodriquez", "Chambers", "Holt", "Lambert", "Fletcher", "Watts", "Bates", "Hale", "Rhodes", "Pena", "Beck", "Newman", "Haynes", "McDaniel", "Mendez", "Bush", "Vaughn", "Parks", "Dawson", "Santiago", "Norris", "Hardy", "Love", "Steele", "Curry", "Powers", "Schultz", "Barker", "Guzman", "Page", "Munoz", "Ball", "Keller", "Chandler", "Weber", "Leonard", "Walsh", "Lyons", "Ramsey", "Wolfe", "Schneider", "Mullins", "Benson", "Sharp", "Bowen", "Daniel", "Barber", "Cummings", "Hines", "Baldwin", "Griffith", "Valdez", "Hubbard", "Salazar", "Reeves", "Warner", "Stevenson", "Burgess", "Santos", "Tate", "Cross", "Garner", "Mann", "Mack", "Moss", "Thornton", "Dennis", "McGee", "Farmer", "Delgado", "Aguilar", "Vega", "Glover", "Manning", "Cohen", "Harmon", "Rodgers", "Robbins", "Newton", "Todd", "Blair", "Higgins", "Ingram", "Reese", "Cannon", "Strickland", "Townsend", "Potter", "Goodwin", "Walton", "Rowe", "Hampton", "Ortega", "Patton", "Swanson", "Joseph", "Francis", "Goodman", "Maldonado", "Yates", "Becker", "Erickson", "Hodges", "Rios", "Conner", "Adkins", "Webster", "Norman", "Malone", "Hammond", "Flowers", "Cobb", "Moody", "Quinn", "Blake", "Maxwell", "Pope", "Floyd", "Osborne", "Paul", "McCarthy", "Guerrero", "Lindsey", "Estrada", "Sandoval", "Gibbs", "Tyler", "Gross", "Fitzgerald", "Stokes", "Doyle", "Sherman", "Saunders", "Wise", "Colon", "Gill", "Alvarado", "Greer", "Padilla", "Simon", "Waters", "Nunez", "Ballard", "Schwartz", "McBride", "Houston", "Christensen", "Klein", "Pratt", "Briggs", "Parsons", "McLaughlin", "Zimmerman", "French", "Buchanan", "Moran", "Copeland", "Roy", "Pittman", "Brady", "McCormick", "Holloway", "Brock", "Poole", "Frank", "Logan", "Owen", "Bass", "Marsh", "Drake", "Wong", "Jefferson", "Park", "Morton", "Abbott", "Sparks", "Patrick", "Norton", "Huff", "Clayton", "Massey", "Lloyd", "Figueroa", "Carson", "Bowers", "Roberson", "Barton", "Tran", "Lamb", "Harrington", "Casey", "Boone", "Cortez", "Clarke", "Mathis", "Singleton", "Wilkins", "Cain", "Bryan", "Underwood", "Hogan", "McKenzie", "Collier", "Luna", "Phelps", "McGuire", "Allison", "Bridges", "Wilkerson", "Nash", "Summers", "Atkins"}

var jobtitles = []string{"Airline Pilot", "Academic Team", "Accountant", "Account Executive", "Actor", "Actuary", "Acquisition Analyst", "Administrative Asst.", "Administrative Analyst", "Administrator", "Advertising Director", "Aerospace Engineer", "Agent", "Agricultural Inspector", "Agricultural Scientist", "Air Traffic Controller", "Animal Trainer", "Anthropologist", "Appraiser", "Architect", "Art Director", "Artist", "Astronomer", "Athletic Coach", "Auditor", "Author", "Baker", "Banker", "Bankruptcy Attorney", "Benefits Manager", "Biologist", "Bio-feedback Specialist", "Biomedical Engineer", "Biotechnical Researcher", "Broadcaster", "Broker", "Building Manager", "Building Contractor", "Building Inspector", "Business Analyst", "Business Planner", "Business Manager", "Buyer", "Call Center Manager", "Career Counselor", "Cash Manager", "Ceramic Engineer", "Chief Executive Officer", "Chief Operation Officer", "Chef", "Chemical Engineer", "Chemist", "Child Care Manager", "Chief Medical Officer", "Chiropractor", "Cinematographer", "City Housing Manager", "City Manager", "Civil Engineer", "Claims Manager", "Clinical Research Assistant", "Collections Manager.", "Compliance Manager", "Comptroller", "Computer Manager", "Commercial Artist", "Communications Affairs Director", "Communications Director", "Communications Engineer", "Compensation Analyst", "Computer Programmer", "Computer Ops. Manager", "Computer Engineer", "Computer Operator", "Computer Graphics Specialist", "Construction Engineer", "Construction Manager", "Consultant", "Consumer Relations Manager", "Contract Administrator", "Copyright Attorney", "Copywriter", "Corporate Planner", "Corrections Officer", "Cosmetologist", "Credit Analyst", "Cruise Director", "Chief Information Officer", "Chief Technology Officer", "Customer Service Manager", "Cryptologist", "Dancer", "Data Security Manager", "Database Manager", "Day Care Instructor", "Dentist", "Designer", "Design Engineer", "Desktop Publisher", "Developer", "Development Officer", "Diamond Merchant", "Dietitian", "Direct Marketer", "Director", "Distribution Manager", "Diversity Manager", "Economist", "EEO Compliance Manager", "Editor", "Education Adminator", "Electrical Engineer", "Electro Optical Engineer", "Electronics Engineer", "Embassy Management", "Employment Agent", "Engineer Technician", "Entrepreneur", "Environmental Analyst", "Environmental Attorney", "Environmental Engineer", "Environmental Specialist", "Escrow Officer", "Estimator", "Executive Assistant", "Executive Director", "Executive Recruiter", "Facilities Manager", "Family Counselor", "Fashion Events Manager", "Fashion Merchandiser", "Fast Food Manager", "Film Producer", "Film Production Assistant", "Financial Analyst", "Financial Planner", "Financier", "Fine Artist", "Wildlife Specialist", "Fitness Consultant", "Flight Attendant", "Flight Engineer", "Floral Designer", "Food & Beverage Director", "Food Service Manager", "Forestry Technician", "Franchise Management", "Franchise Sales", "Fraud Investigator", "Freelance Writer", "Fund Raiser", "General Manager", "Geologist", "General Counsel", "Geriatric Specialist", "Gerontologist", "Glamour Photographer", "Golf Club Manager", "Gourmet Chef", "Graphic Designer", "Grounds Keeper", "Hazardous Waste Manager", "Health Care Manager", "Health Therapist", "Health Service Administrator", "Hearing Officer", "Home Economist", "Horticulturist", "Hospital Administrator", "Hotel Manager", "Human Resources Manager", "Importer", "Industrial Designer", "Industrial Engineer", "Information Director", "Inside Sales", "Insurance Adjuster", "Interior Decorator", "Internal Controls Director", "International Acct.", "International Courier", "International Lawyer", "Interpreter", "Investigator", "Investment Banker", "Investment Manager", "IT Architect", "IT Project Manager", "IT Systems Analyst", "Jeweler", "Joint Venture Manager", "Journalist", "Labor Negotiator", "Labor Organizer", "Labor Relations Manager", "Lab Services Director", "Lab Technician", "Land Developer", "Landscape Architect", "Law Enforcement Officer", "Lawyer", "Lead Software Engineer", "Lead Software Test Engineer", "Leasing Manager", "Legal Secretary", "Library Manager", "Litigation Attorney", "Loan Officer", "Lobbyist", "Logistics Manager", "Maintenance Manager", "Management Consultant", "Managed Care Director", "Managing Partner", "Manufacturing Director", "Manpower Planner", "Marine Biologist", "Market Res. Analyst", "Marketing Director", "Materials Manager", "Mathematician", "Membership Chairman", "Mechanic", "Mechanical Engineer", "Media Buyer", "Medical Investor", "Medical Secretary", "Medical Technician", "Mental Health Counselor", "Merchandiser", "Metallurgical Engineering", "Meteorologist", "Microbiologist", "MIS Manager", "Motion Picture Director", "Multimedia Director", "Musician", "Network Administrator", "Network Specialist", "Network Operator", "New Product Manager", "Novelist", "Nuclear Engineer", "Nuclear Specialist", "Nutritionist", "Nursing Administrator", "Occupational Therapist", "Oceanographer", "Office Manager", "Operations Manager", "Operations Research Director", "Optical Technician", "Optometrist", "Organizational Development Manager", "Outplacement Specialist", "Paralegal", "Park Ranger", "Patent Attorney", "Payroll Specialist", "Personnel Specialist", "Petroleum Engineer", "Pharmacist", "Photographer", "Physical Therapist", "Physician", "Physician Assistant", "Physicist", "Planning Director", "Podiatrist", "Political Analyst", "Political Scientist", "Politician", "Portfolio Manager", "Preschool Management", "Preschool Teacher", "Principal", "Private Banker", "Private Investigator", "Probation Officer", "Process Engineer", "Producer", "Product Manager", "Product Engineer", "Production Engineer", "Production Planner", "Professional Athlete", "Professional Coach", "Professor", "Project Engineer", "Project Manager", "Program Manager", "Property Manager", "Public Administrator", "Public Safety Director", "PR Specialist", "Publisher", "Purchasing Agent", "Publishing Director", "Quality Assurance Specialist", "Quality Control Engineer", "Quality Control Inspector", "Radiology Manager", "Railroad Engineer", "Real Estate Broker", "Recreational Director", "Recruiter", "Redevelopment Specialist", "Regulatory Affairs Manager", "Registered Nurse", "Rehabilitation Counselor", "Relocation Manager", "Reporter", "Research Specialist", "Restaurant Manager", "Retail Store Manager", "Risk Analyst", "Safety Engineer", "Sales Engineer", "Sales Trainer", "Sales Promotion Manager", "Sales Representative", "Sales Manager", "Service Manager", "Sanitation Engineer", "Scientific Programmer", "Scientific Writer", "Securities Analyst", "Security Consultant", "Security Director", "Seminar Presenter", "Ship's Officer", "Singer", "Social Director", "Social Program Planner", "Social Research", "Social Scientist", "Social Worker", "Sociologist", "Software Developer", "Software Engineer", "Software Test Engineer", "Soil Scientist", "Special Events Manager", "Special Education Teacher", "Special Projects Director", "Speech Pathologist", "Speech Writer", "Sports Event Manager", "Statistician", "Store Manager", "Strategic Alliance Director", "Strategic Planning Director", "Stress Reduction Specialist", "Stockbroker", "Surveyor", "Structural Engineer", "Superintendent", "Supply Chain Director", "System Engineer", "Systems Analyst", "Systems Programmer", "System Administrator", "Tax Specialist", "Teacher", "Technical Support Specialist", "Technical Illustrator", "Technical Writer", "Technology Director", "Telecom Analyst", "Telemarketer", "Theatrical Director", "Title Examiner", "Tour Escort", "Tour Guide Director", "Traffic Manager", "Trainer Translator", "Transportation Manager", "Travel Agent", "Treasurer", "TV Programmer", "Underwriter", "Union Representative", "University Administrator", "University Dean", "Urban Planner", "Veterinarian", "Vendor Relations Director", "Viticulturist", "Warehouse Manager"}

func PersonEmail() string {
	return New().PersonEmail()
}

func (f *Faker) PersonEmail() string {
	return fmt.Sprintf("%s.%s@%s",
		strings.ToLower(f.PersonFirstname()),
		strings.ToLower(f.PersonLastname()),
		f.Domain(),
	)
}

func PersonPhone() string {
	return New().PersonPhone()
}

func (f *Faker) PersonPhone() string {
	return fmt.Sprintf("%05d %03d %03d",
		f.IntegerBetween(0, 9999),
		f.IntegerBetween(0, 999),
		f.IntegerBetween(0, 999),
	)
}

func PersonGender() string {
	return New().PersonGender()
}

func (f *Faker) PersonGender() string {
	return gender[f.r.Intn(len(gender))]
}

func PersonFullname() string {
	return New().PersonFullname()
}

func (f *Faker) PersonFullname() string {
	return f.PersonFirstname() + " " + f.PersonLastname()
}

func PersonFirstname() string {
	return New().PersonFirstname()
}

func (f *Faker) PersonFirstname() string {
	switch 0.5 <= f.r.Float64() {
	case true:
		return male[f.r.Intn(len(male))]
	case false:
		return female[f.r.Intn(len(female))]
	}
	return ""
}

func PersonLastname() string {
	return New().PersonLastname()
}

func (f *Faker) PersonLastname() string {
	return surnames[f.r.Intn(len(surnames))]
}

func PersonUsername() string {
	return New().PersonUsername()
}

func (f *Faker) PersonUsername() string {
	return fmt.Sprintf("%s.%s%d",
		strings.ToLower(f.PersonFirstname()),
		strings.ToLower(f.PersonLastname()),
		f.IntegerBetween(1, 999),
	)
}

func PersonJobtitle() string {
	return New().PersonJobtitle()
}

func (f *Faker) PersonJobtitle() string {
	return jobtitles[f.r.Intn(len(jobtitles))]
}

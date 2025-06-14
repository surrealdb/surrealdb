pub mod barcode {
	use crate::err::Error;
	use crate::sql::value::Value;

	use fake::{Fake, faker::barcode::en::*};

	pub fn isbn(_: ()) -> Result<Value, Error> {
		let val: String = Isbn().fake();
		Ok(val.into())
	}

	pub fn isbn10(_: ()) -> Result<Value, Error> {
		let val: String = Isbn10().fake();
		Ok(val.into())
	}

	pub fn isbn13(_: ()) -> Result<Value, Error> {
		let val: String = Isbn13().fake();
		Ok(val.into())
	}
}

pub mod company {
	use crate::err::Error;
	use crate::sql::value::Value;

	use fake::{Fake, faker::company::en::*};

	pub fn name(_: ()) -> Result<Value, Error> {
		let val: String = CompanyName().fake();
		Ok(val.into())
	}

	pub fn suffix(_: ()) -> Result<Value, Error> {
		let val: String = CompanySuffix().fake();
		Ok(val.into())
	}

	pub fn tagline(_: ()) -> Result<Value, Error> {
		let val: String = CatchPhrase().fake();
		Ok(val.into())
	}

	pub fn industry(_: ()) -> Result<Value, Error> {
		let val: String = Industry().fake();
		Ok(val.into())
	}

	pub fn profession(_: ()) -> Result<Value, Error> {
		let val: String = Profession().fake();
		Ok(val.into())
	}
}

pub mod currency {
	use crate::err::Error;
	use crate::sql::value::Value;

	use fake::{Fake, faker::currency::en::*};

	pub fn code(_: ()) -> Result<Value, Error> {
		let val: String = CurrencyCode().fake();
		Ok(val.into())
	}

	pub fn name(_: ()) -> Result<Value, Error> {
		let val: String = CurrencyName().fake();
		Ok(val.into())
	}

	pub fn symbol(_: ()) -> Result<Value, Error> {
		let val: String = CurrencySymbol().fake();
		Ok(val.into())
	}
}

pub mod file {
	use crate::err::Error;
	use crate::sql::value::Value;

	use fake::{Fake, faker::filesystem::en::*};

	pub fn dir_path(_: ()) -> Result<Value, Error> {
		let val: String = DirPath().fake();
		Ok(val.into())
	}

	pub fn extension(_: ()) -> Result<Value, Error> {
		let val: String = FileExtension().fake();
		Ok(val.into())
	}

	pub fn name(_: ()) -> Result<Value, Error> {
		let val: String = FileName().fake();
		Ok(val.into())
	}

	pub fn path(_: ()) -> Result<Value, Error> {
		let val: String = FilePath().fake();
		Ok(val.into())
	}
}

pub mod finance {
	use crate::err::Error;
	use crate::sql::value::Value;

	use fake::{Fake, faker::creditcard::en::*, faker::finance::en::*};

	pub fn credit_card(_: ()) -> Result<Value, Error> {
		let val: String = CreditCardNumber().fake();
		Ok(val.into())
	}

	pub fn bic(_: ()) -> Result<Value, Error> {
		let val: String = Bic().fake();
		Ok(val.into())
	}

	pub fn isin(_: ()) -> Result<Value, Error> {
		let val: String = Isin().fake();
		Ok(val.into())
	}
}

pub mod internet {
	use crate::err::Error;
	use crate::sql::value::Value;

	use fake::{Fake, faker::internet::en::*};

	pub fn domain_suffix(_: ()) -> Result<Value, Error> {
		let val: String = DomainSuffix().fake();
		Ok(val.into())
	}

	pub fn free_email(_: ()) -> Result<Value, Error> {
		let val: String = FreeEmail().fake();
		Ok(val.into())
	}

	pub fn email(_: ()) -> Result<Value, Error> {
		let val: String = SafeEmail().fake();
		Ok(val.into())
	}

	pub fn ipv4(_: ()) -> Result<Value, Error> {
		let val: String = IPv4().fake();
		Ok(val.into())
	}

	pub fn ipv6(_: ()) -> Result<Value, Error> {
		let val: String = IPv6().fake();
		Ok(val.into())
	}

	pub fn ip(_: ()) -> Result<Value, Error> {
		let val: String = IP().fake();
		Ok(val.into())
	}

	pub fn mac_address(_: ()) -> Result<Value, Error> {
		let val: String = MACAddress().fake();
		Ok(val.into())
	}

	pub fn username(_: ()) -> Result<Value, Error> {
		let val: String = Username().fake();
		Ok(val.into())
	}

	pub fn password(_: ()) -> Result<Value, Error> {
		let val: String = Password(std::ops::Range::from(8..16)).fake();
		Ok(val.into())
	}

	pub fn user_agent(_: ()) -> Result<Value, Error> {
		let val: String = UserAgent().fake();
		Ok(val.into())
	}
}

pub mod name {
	use crate::err::Error;
	use crate::sql::value::Value;

	use fake::{Fake, faker::name::en::*};

	pub fn first_name(_: ()) -> Result<Value, Error> {
		let val: String = FirstName().fake();
		Ok(val.into())
	}

	pub fn last_name(_: ()) -> Result<Value, Error> {
		let val: String = LastName().fake();
		Ok(val.into())
	}

	pub fn full_name(_: ()) -> Result<Value, Error> {
		let val: String = Name().fake();
		Ok(val.into())
	}

	pub fn suffix(_: ()) -> Result<Value, Error> {
		let val: String = Suffix().fake();
		Ok(val.into())
	}

	pub fn title(_: ()) -> Result<Value, Error> {
		let val: String = Title().fake();
		Ok(val.into())
	}
}

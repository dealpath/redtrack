require 'spec_helper'
require 'bigdecimal'

describe RedTrack::DataTypes do

  let (:redtrack_datatypes) {
    RedTrack::DataTypes.new({})
  }

  let (:fake_big_decimal) {
    BigDecimal.new("0.0001")
  }

  let(:fake_hash_object) {
    data = {
        test_integer: 1,
        test_string: 'some string'
    }
    data
  }

  let(:valid_timestamp_with_tz) {
    Time.new(2014,12,05, 13,30,0, "+09:00")
  }

  let(:valid_date) {
    Date.new(2014,12,05)
  }

  context '[Public Methods]' do
    context 'check_smallint()' do

      it 'should accept valid integer' do
        result = redtrack_datatypes.check_smallint(1)
        expect(result).to eq(1)
      end
      it 'should accept valid negative integer' do
        result = redtrack_datatypes.check_smallint(-1)
        expect(result).to eq(-1)
      end
      it 'should reject out of range integer' do
        expect { redtrack_datatypes.check_smallint(RedTrack::Datatypes::MAX_SMALLINT + 1) }.to raise_error
      end
      it 'should reject out of range negative integer' do
        expect { redtrack_datatypes.check_smallint(RedTrack::Datatypes::MIN_SMALLINT - 1) }.to raise_error
      end
      it 'should reject float value' do
        expect { redtrack_datatypes.check_smallint(1.1) }.to raise_error
      end
      it 'should reject boolean value' do
        expect { redtrack_datatypes.check_smallint(true) }.to raise_error
      end
      it 'should reject non-numeric string' do
        expect { redtrack_datatypes.check_smallint("a") }.to raise_error
      end
      it 'should reject string with integer in it' do
        expect { redtrack_datatypes.check_smallint("1") }.to raise_error
      end
      it 'should reject date' do
        expect { redtrack_datatypes.check_smallint(Date.today)}.to raise_error
      end
      it 'should reject timestamp' do
        expect { redtrack_datatypes.check_smallint(Time.now) }.to raise_error
      end
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_smallint(fake_hash_object) }.to raise_error
      end
    end

    context 'check_integer()' do
      it 'should accept valid integer' do
        result = redtrack_datatypes.check_integer(1)
        expect(result).to eq(1)
      end
      it 'should accept valid negative integer' do
        result = redtrack_datatypes.check_integer(-1)
        expect(result).to eq(-1)
      end
      it 'should reject out of range integer' do
        expect { redtrack_datatypes.check_integer(RedTrack::Datatypes::MAX_INTEGER + 1) }.to raise_error
      end
      it 'should reject out of range negative integer' do
        expect { redtrack_datatypes.check_integer(RedTrack::Datatypes::MIN_INTEGER - 1) }.to raise_error
      end
      it 'should reject non-integer numeric value' do
        expect { redtrack_datatypes.check_integer(1.1) }.to raise_error
      end
      it 'should reject boolean value' do
        expect { redtrack_datatypes.check_integer(true) }.to raise_error
      end
      it 'should reject non-numeric string' do
        expect { redtrack_datatypes.check_integer("a") }.to raise_error
      end
      it 'should reject string with integer in it' do
        expect { redtrack_datatypes.check_integer("1") }.to raise_error
      end
      it 'should reject date' do
        expect { redtrack_datatypes.check_integer(Date.today)}.to raise_error
      end
      it 'should reject timestamp' do
        expect { redtrack_datatypes.check_integer(Time.now) }.to raise_error
      end
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_integer(fake_hash_object) }.to raise_error
      end
    end

    context 'check_bigint()' do
      it 'should accept valid integer' do
        result = redtrack_datatypes.check_bigint(1)
        expect(result).to eq(1)
      end
      it 'should accept valid negative integer' do
        result = redtrack_datatypes.check_bigint(-1)
        expect(result).to eq(-1)
      end
      it 'should reject out of range integer' do
        expect { redtrack_datatypes.check_bigint(RedTrack::Datatypes::MAX_BIGINT + 1) }.to raise_error
      end
      it 'should reject out of range negative integer' do
        expect { redtrack_datatypes.check_bigint(RedTrack::Datatypes::MIN_BIGINT - 1) }.to raise_error
      end
      it 'should reject float value' do
        expect { redtrack_datatypes.check_bigint(1.1) }.to raise_error
      end
      it 'should reject boolean value' do
        expect { redtrack_datatypes.check_bigint(true) }.to raise_error
      end
      it 'should reject non-numeric string' do
        expect { redtrack_datatypes.check_bigint("a") }.to raise_error
      end
      it 'should reject string with integer in it' do
        expect { redtrack_datatypes.check_bigint("1") }.to raise_error
      end
      it 'should reject date' do
        expect { redtrack_datatypes.check_bigint(Date.today)}.to raise_error
      end
      it 'should reject timestamp' do
        expect { redtrack_datatypes.check_bigint(Time.now) }.to raise_error
      end
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_bigint(fake_hash_object) }.to raise_error
      end
    end

    context 'check_decimal()' do
      it 'should accept big decimal' do
        result = redtrack_datatypes.check_decimal(fake_big_decimal)
        expect(result). to eq(fake_big_decimal)
      end
      it 'should reject integer' do
        expect { redtrack_datatypes.check_decimal(1) }.to raise_error
      end
      it 'should reject valid float value' do
        expect { redtrack_datatypes.check_decimal(1.1) }.to raise_error
      end
      it 'should reject boolean value' do
        expect { redtrack_datatypes.check_decimal(true) }.to raise_error
      end
      it 'should reject non-numeric string' do
        expect { redtrack_datatypes.check_decimal("a") }.to raise_error
      end
      it 'should reject strings with integer in it' do
        expect { redtrack_datatypes.check_decimal("1") }.to raise_error
      end
      it 'should reject date' do
        expect { redtrack_datatypes.check_decimal(Date.today)}.to raise_error
      end
      it 'should reject timestamp' do
        expect { redtrack_datatypes.check_decimal(Time.now) }.to raise_error
      end
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_decimal(fake_hash_object) }.to raise_error
      end
    end

    context 'check_real()' do
      it 'should accept valid integer' do
        result = redtrack_datatypes.check_real(1)
        expect(result).to eq(1)
      end
      it 'should accept valid float value' do
        result = redtrack_datatypes.check_real(1.1)
        expect(result).to eq(1.1)
      end
      it 'should reject boolean value' do
        expect { redtrack_datatypes.check_real(true) }.to raise_error
      end
      it 'should reject non-numeric string' do
        expect { redtrack_datatypes.check_real("a") }.to raise_error
      end
      it 'should reject strings with integer in it' do
        expect { redtrack_datatypes.check_real("1") }.to raise_error
      end
      it 'should reject date' do
        expect { redtrack_datatypes.check_real(Date.today)}.to raise_error
      end
      it 'should reject timestamp' do
        expect { redtrack_datatypes.check_real(Time.now) }.to raise_error
      end
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_real(fake_hash_object) }.to raise_error
      end
    end

    context 'check_double_precision()' do
      it 'should accept valid integer' do
        result = redtrack_datatypes.check_double_precision(1)
        expect(result).to eq(1)
      end
      it 'should accept valid float value' do
        result = redtrack_datatypes.check_double_precision(1.1)
        expect(result).to eq(1.1)
      end
      it 'should reject boolean value' do
        expect { redtrack_datatypes.check_double_precision(true) }.to raise_error
      end
      it 'should reject non-numeric string' do
        expect { redtrack_datatypes.check_double_precision("a") }.to raise_error
      end
      it 'should reject strings with integer in it' do
        expect { redtrack_datatypes.check_double_precision("1") }.to raise_error
      end
      it 'should reject date' do
        expect { redtrack_datatypes.check_double_precision(Date.today)}.to raise_error
      end
      it 'should reject timestamp' do
        expect { redtrack_datatypes.check_double_precision(Time.now) }.to raise_error
      end
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_double_precision(fake_hash_object) }.to raise_error
      end
    end

    context 'check_boolean()' do
      it 'should accept boolean value' do
        result = redtrack_datatypes.check_boolean(true)
        expect(result).to eq(true)
      end
      it 'should reject integer' do
        expect { redtrack_datatypes.check_boolean(1,"varchar(4)")}.to raise_error
      end
      it 'should reject float' do
        expect { redtrack_datatypes.check_boolean(1.1,"varchar(4)") }.to raise_error
      end
      it 'should reject non-numeric string' do
        expect { redtrack_datatypes.check_boolean("a") }.to raise_error
      end
      it 'should reject strings with integer in it' do
        expect { redtrack_datatypes.check_boolean("1") }.to raise_error
      end
      it 'should reject date' do
        expect { redtrack_datatypes.check_boolean(Date.today)}.to raise_error
      end
      it 'should reject timestamp' do
        expect { redtrack_datatypes.check_boolean(Time.now) }.to raise_error
      end
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_boolean(fake_hash_object) }.to raise_error
      end
    end

    context 'check_char()' do
      it 'should accept valid string' do
        result = redtrack_datatypes.check_char("test","char(4)")
        expect(result).to eq ("test")
      end
      it 'should accept & truncate long string' do
        result = redtrack_datatypes.check_char("testtest","char(4)")
        expect(result).to eq ("test")
      end
      it 'should reject integer' do
        expect { redtrack_datatypes.check_char(1,"char(4)")}.to raise_error
      end
      it 'should reject float' do
        expect { redtrack_datatypes.check_char(1.1,"char(4)") }.to raise_error
      end
      it 'should reject boolean value' do
        expect { redtrack_datatypes.check_char(true) }.to raise_error
      end
      it 'should reject date' do
        expect { redtrack_datatypes.check_char(Date.today)}.to raise_error
      end
      it 'should reject timestamp' do
        expect { redtrack_datatypes.check_char(Time.now) }.to raise_error
      end
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_char(fake_hash_object,"char(4)") }.to raise_error
      end
    end

    context 'check_varchar()' do
      it 'should accept valid string' do
        result = redtrack_datatypes.check_varchar("test","varchar(4)")
        expect(result).to eq ("test")
      end
      it 'should accept & truncate long string' do
        result = redtrack_datatypes.check_varchar("testtest","varchar(4)")
        expect(result).to eq ("test")
      end
      it 'should reject integer' do
        expect { redtrack_datatypes.check_varchar(1,"varchar(4)")}.to raise_error
      end
      it 'should reject float' do
        expect { redtrack_datatypes.check_varchar(1.1,"varchar(4)") }.to raise_error
      end
      it 'should reject boolean value' do
        expect { redtrack_datatypes.check_varchar(true) }.to raise_error
      end
      it 'should reject date' do
        expect { redtrack_datatypes.check_varchar(Date.today)}.to raise_error
      end
      it 'should reject timestamp' do
        expect { redtrack_datatypes.check_varchar(Time.now) }.to raise_error
      end
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_varchar(fake_hash_object,"varchar(4)") }.to raise_error
      end
    end

    context 'check_date()' do
      it 'should accept date' do
        result = redtrack_datatypes.check_date(valid_date)
        expect(result).to eq('2014-12-05')
      end
      it 'should reject string' do
        expect { redtrack_datatypes.check_date("test","varchar(4)") }.to raise_error
      end
      it 'should reject integer' do
        expect { redtrack_datatypes.check_date(1,"varchar(4)")}.to raise_error
      end
      it 'should reject float' do
        expect { redtrack_datatypes.check_date(1.1,"varchar(4)") }.to raise_error
      end
      it 'should reject boolean value' do
        expect { redtrack_datatypes.check_date(true) }.to raise_error
      end
      it 'should reject timestamp' do
        expect { redtrack_datatypes.check_date(Time.now) }.to raise_error
      end
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_date(fake_hash_object,"varchar(4)") }.to raise_error
      end
    end

    context 'check_timestamp()' do
      it 'should accept valid timestamp (utc) and convert to redshifts timezone-less format' do
        result = redtrack_datatypes.check_timestamp(valid_timestamp_with_tz.utc)
        expect(result).to eq('2014-12-05 04:30:00')
      end
      it 'should accept valid timestamp w/ timezone and convert to redshifts timezone-less format without converting timezone' do
        result = redtrack_datatypes.check_timestamp(valid_timestamp_with_tz)
        expect(result).to eq('2014-12-05 13:30:00')
      end
      it 'should reject string' do
        expect { redtrack_datatypes.check_timestamp("test","varchar(4)") }.to raise_error
      end
      it 'should reject integer' do
        expect { redtrack_datatypes.check_timestamp(1)}.to raise_error
      end
      it 'should reject float' do
        expect { redtrack_datatypes.check_timestamp(1.1) }.to raise_error
      end
      it 'should reject boolean' do
        expect { redtrack_datatypes.check_timestamp(true) }.to raise_error
      end
      it 'should reject date' do
        expect { redtrack_datatypes.check_timestamp(Date.today)}.to raise_error
      end
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_timestamp(fake_hash_object,"varchar(4)") }.to raise_error
      end
    end
  end
end
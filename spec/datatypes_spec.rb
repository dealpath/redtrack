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
        expect { redtrack_datatypes.check_varchar(1,"varchar(4)")}.to raise_error
      end
      it 'should reject float' do
        expect { redtrack_datatypes.check_varchar(1.1,"varchar(4)") }.to raise_error
      end
      it 'should reject non-numeric string' do
        expect { redtrack_datatypes.check_double_precision("a") }.to raise_error
      end
      it 'should reject strings with integer in it' do
        expect { redtrack_datatypes.check_double_precision("1") }.to raise_error
      end
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_double_precision(fake_hash_object) }.to raise_error
      end
    end

    context 'check_char()' do
      it 'should accept valid string' do
        result = redtrack_datatypes.check_char("test","char(4)")
        expect(result).to eq ("test")
      end
      it 'should truncate long string' do
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
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_char(fake_hash_object,"char(4)") }.to raise_error
      end
    end

    context 'check_varchar()' do
      it 'should accept valid string' do
        result = redtrack_datatypes.check_varchar("test","varchar(4)")
        expect(result).to eq ("test")
      end
      it 'should truncate long string' do
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
      it 'should reject hash object' do
        expect { redtrack_datatypes.check_varchar(fake_hash_object,"varchar(4)") }.to raise_error
      end
    end

    context 'check_date()' do
      # todo implement tests
    end

    context 'check_timestamp()' do
      # todo implement tests
    end

  end
end
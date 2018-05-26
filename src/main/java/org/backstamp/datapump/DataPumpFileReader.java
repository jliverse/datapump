package org.backstamp.datapump;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.backstamp.datapump.row.MasterTableRow;
import org.backstamp.datapump.row.TableRow;
import org.backstamp.datapump.util.Reader;
import org.backstamp.datapump.util.Value.Once;

public interface DataPumpFileReader {

	public String versionName();

	public String characterSet();

	public Date date();

	public long blockSize();

	public boolean master();

	public DataPumpTable<MasterTableRow> masterTable();

	public DataPumpTable<TableRow> tableWithName(String name);

	public DataPumpTable<TableRow> tableMatching(Predicate<MasterTableRow> predicate);

	public class FileVersion extends Once<Integer> {
		public FileVersion(Reader reader) {
			super(() -> reader.read2());
		}

		public String description() {
			switch (value().orElse(0)) {
			case 0x0001: return "Oracle 10g Release 1: 10.1.0";
			case 0x0101: return "Oracle 10g Release 2: 10.2.0";
			case 0x0201: return "Oracle 11g Release 1: 11.1.0";
			case 0x0301: return "Oracle 11g Release 2: 11.2.0";
			case 0x0401: return "Oracle 12c Release 1: 12.1.0";
			default: throw new RuntimeException("Unsupported format");
			}
		}
	}

	public class MasterPresent extends Once<Boolean> {
		public MasterPresent(Reader reader) {
			super(() -> reader.read2() > 0);
		}
	}

	public class GUID extends Once<String> {
		public GUID(Reader reader) {
			super(() -> {
				byte[] bytes = reader.read(16);
				return IntStream.range(0, bytes.length)
						.mapToObj(j -> Integer.toHexString(Byte.toUnsignedInt(bytes[j])))
						.map(j-> "0".concat(j).substring(j.length() - 1))
						.collect(Collectors.joining());
				
			});
		}

		public String description() {
			return value().get();
		}
	}

	public class FileNumber extends Once<Long> {
		public FileNumber(Reader reader) {
			super(() -> reader.read4());
		}
	}

	public class BlockSize extends Once<Long> {
		public BlockSize(Reader reader) {
			super(() -> reader.read4());
		}
	}

	public class CharacterSet extends Once<Integer> {
		public CharacterSet(Reader reader) {
			super(() -> reader.read2());
		}

		public String description() {
			switch (value().orElse(0)) {
			case 1: return "ASCII"; // US7ASCII
			case 2: return "WE8DEC";
			case 3: return "WE8HP";
			case 4: return "US8PC437";
			case 5: return "WE8EBCDIC37";
			case 6: return "WE8EBCDIC500";
			case 7: return "WE8EBCDIC1140";
			case 8: return "WE8EBCDIC285";
			case 9: return "WE8EBCDIC1146";
			case 10: return "WE8PC850";
			case 11: return "D7DEC";
			case 12: return "F7DEC";
			case 13: return "S7DEC";
			case 14: return "E7DEC";
			case 15: return "SF7ASCII";
			case 16: return "NDK7DEC";
			case 17: return "I7DEC";
			case 18: return "NL7DEC";
			case 19: return "CH7DEC";
			case 20: return "YUG7ASCII";
			case 21: return "SF7DEC";
			case 22: return "TR7DEC";
			case 23: return "IW7IS960";
			case 25: return "IN8ISCII";
			case 27: return "WE8EBCDIC1148";
			case 28: return "WE8PC858";
			case 31: return "WE8ISO8859P1"; // ISO_LATIN_1
			case 32: return "EE8ISO8859P2";
			case 33: return "SE8ISO8859P3";
			case 34: return "NEE8ISO8859P4";
			case 35: return "CL8ISO8859P5";
			case 36: return "AR8ISO8859P6";
			case 37: return "EL8ISO8859P7";
			case 38: return "IW8ISO8859P8";
			case 39: return "WE8ISO8859P9";
			case 40: return "NE8ISO8859P10";
			case 41: return "TH8TISASCII";
			case 42: return "TH8TISEBCDIC";
			case 43: return "BN8BSCII";
			case 44: return "VN8VN3";
			case 45: return "VN8MSWIN1258";
			case 46: return "WE8ISO8859P15";
			case 47: return "BLT8ISO8859P13";
			case 48: return "CEL8ISO8859P14";
			case 49: return "CL8ISOIR111";
			case 50: return "WE8NEXTSTEP";
			case 51: return "CL8KOI8U";
			case 52: return "AZ8ISO8859P9E";
			case 61: return "AR8ASMO708PLUS";
			case 70: return "AR8EBCDICX";
			case 72: return "AR8XBASIC";
			case 81: return "EL8DEC";
			case 82: return "TR8DEC";
			case 90: return "WE8EBCDIC37C";
			case 91: return "WE8EBCDIC500C";
			case 92: return "IW8EBCDIC424";
			case 93: return "TR8EBCDIC1026";
			case 94: return "WE8EBCDIC871";
			case 95: return "WE8EBCDIC284";
			case 96: return "WE8EBCDIC1047";
			case 97: return "WE8EBCDIC1140C";
			case 98: return "WE8EBCDIC1145";
			case 99: return "WE8EBCDIC1148C";
			case 100: return "WE8EBCDIC1047E";
			case 101: return "WE8EBCDIC924";
			case 110: return "EEC8EUROASCI";
			case 113: return "EEC8EUROPA3";
			case 114: return "LA8PASSPORT";
			case 140: return "BG8PC437S";
			case 150: return "EE8PC852";
			case 152: return "RU8PC866";
			case 153: return "RU8BESTA";
			case 154: return "IW8PC1507";
			case 155: return "RU8PC855";
			case 156: return "TR8PC857";
			case 158: return "CL8MACCYRILLIC";
			case 159: return "CL8MACCYRILLICS";
			case 160: return "WE8PC860";
			case 161: return "IS8PC861";
			case 162: return "EE8MACCES";
			case 163: return "EE8MACCROATIANS";
			case 164: return "TR8MACTURKISHS";
			case 165: return "IS8MACICELANDICS";
			case 166: return "EL8MACGREEKS";
			case 167: return "IW8MACHEBREWS";
			case 170: return "EE8MSWIN1250";
			case 171: return "CL8MSWIN1251";
			case 172: return "ET8MSWIN923";
			case 173: return "BG8MSWIN";
			case 174: return "EL8MSWIN1253";
			case 175: return "IW8MSWIN1255";
			case 176: return "LT8MSWIN921";
			case 177: return "TR8MSWIN1254";
			case 178: return "WE8MSWIN1252";
			case 179: return "BLT8MSWIN1257";
			case 180: return "D8EBCDIC273";
			case 181: return "I8EBCDIC280";
			case 182: return "DK8EBCDIC277";
			case 183: return "S8EBCDIC278";
			case 184: return "EE8EBCDIC870";
			case 185: return "CL8EBCDIC1025";
			case 186: return "F8EBCDIC297";
			case 187: return "IW8EBCDIC1086";
			case 188: return "CL8EBCDIC1025X";
			case 189: return "D8EBCDIC1141";
			case 190: return "N8PC865";
			case 191: return "BLT8CP921";
			case 192: return "LV8PC1117";
			case 193: return "LV8PC8LR";
			case 194: return "BLT8EBCDIC1112";
			case 195: return "LV8RST104090";
			case 196: return "CL8KOI8R";
			case 197: return "BLT8PC775";
			case 198: return "DK8EBCDIC1142";
			case 199: return "S8EBCDIC1143";
			case 200: return "I8EBCDIC1144";
			case 201: return "F7SIEMENS9780X";
			case 202: return "E7SIEMENS9780X";
			case 203: return "S7SIEMENS9780X";
			case 204: return "DK7SIEMENS9780X";
			case 205: return "N7SIEMENS9780X";
			case 206: return "I7SIEMENS9780X";
			case 207: return "D7SIEMENS9780X";
			case 208: return "F8EBCDIC1147";
			case 210: return "WE8GCOS7";
			case 211: return "EL8GCOS7";
			case 221: return "US8BS2000";
			case 222: return "D8BS2000";
			case 223: return "F8BS2000";
			case 224: return "E8BS2000";
			case 225: return "DK8BS2000";
			case 226: return "S8BS2000";
			case 230: return "WE8BS2000E";
			case 231: return "WE8BS2000";
			case 232: return "EE8BS2000";
			case 233: return "CE8BS2000";
			case 235: return "CL8BS2000";
			case 239: return "WE8BS2000L5";
			case 241: return "WE8DG";
			case 251: return "WE8NCR4970";
			case 261: return "WE8ROMAN8";
			case 262: return "EE8MACCE";
			case 263: return "EE8MACCROATIAN";
			case 264: return "TR8MACTURKISH";
			case 265: return "IS8MACICELANDIC";
			case 266: return "EL8MACGREEK";
			case 267: return "IW8MACHEBREW";
			case 277: return "US8ICL";
			case 278: return "WE8ICL";
			case 279: return "WE8ISOICLUK";
			case 301: return "EE8EBCDIC870C";
			case 311: return "EL8EBCDIC875S";
			case 312: return "TR8EBCDIC1026S";
			case 314: return "BLT8EBCDIC1112S";
			case 315: return "IW8EBCDIC424S";
			case 316: return "EE8EBCDIC870S";
			case 317: return "CL8EBCDIC1025S";
			case 319: return "TH8TISEBCDICS";
			case 320: return "AR8EBCDIC420S";
			case 322: return "CL8EBCDIC1025C";
			case 323: return "CL8EBCDIC1025R";
			case 324: return "EL8EBCDIC875R";
			case 325: return "CL8EBCDIC1158";
			case 326: return "CL8EBCDIC1158R";
			case 327: return "EL8EBCDIC423R";
			case 351: return "WE8MACROMAN8";
			case 352: return "WE8MACROMAN8S";
			case 353: return "TH8MACTHAI";
			case 354: return "TH8MACTHAIS";
			case 368: return "HU8CWI2";
			case 380: return "EL8PC437S";
			case 381: return "EL8EBCDIC875";
			case 382: return "EL8PC737";
			case 383: return "LT8PC772";
			case 384: return "LT8PC774";
			case 385: return "EL8PC869";
			case 386: return "EL8PC851";
			case 390: return "CDN8PC863";
			case 401: return "HU8ABMOD";
			case 500: return "AR8ASMO8X";
			case 504: return "AR8NAFITHA711T";
			case 505: return "AR8SAKHR707T";
			case 506: return "AR8MUSSAD768T";
			case 507: return "AR8ADOS710T";
			case 508: return "AR8ADOS720T";
			case 509: return "AR8APTEC715T";
			case 511: return "AR8NAFITHA721T";
			case 514: return "AR8HPARABIC8T";
			case 554: return "AR8NAFITHA711";
			case 555: return "AR8SAKHR707";
			case 556: return "AR8MUSSAD768";
			case 557: return "AR8ADOS710";
			case 558: return "AR8ADOS720";
			case 559: return "AR8APTEC715";
			case 560: return "AR8MSWIN1256"; // AR8MSAWIN
			case 561: return "AR8NAFITHA721";
			case 563: return "AR8SAKHR706";
			case 565: return "AR8ARABICMAC";
			case 566: return "AR8ARABICMACS";
			case 567: return "AR8ARABICMACT";
			case 590: return "LA8ISO6937";
			case 797: return "US8NOOP";
			case 798: return "WE8DECTST";
			case 829: return "JA16VMS";
			case 830: return "JA16EUC";
			case 831: return "JA16EUCYEN";
			case 832: return "JA16SJIS";
			case 833: return "JA16DBCS";
			case 834: return "JA16SJISYEN";
			case 835: return "JA16EBCDIC930";
			case 836: return "JA16MACSJIS";
			case 837: return "JA16EUCTILDE";
			case 838: return "JA16SJISTILDE";
			case 840: return "KO16KSC5601";
			case 842: return "KO16DBCS";
			case 845: return "KO16KSCCS";
			case 846: return "KO16MSWIN949";
			case 850: return "ZHS16CGB231280";
			case 851: return "ZHS16MACCGB231280";
			case 852: return "ZHS16GBK";
			case 853: return "ZHS16DBCS";
			case 854: return "ZHS32GB18030";
			case 860: return "ZHT32EUC";
			case 861: return "ZHT32SOPS";
			case 862: return "ZHT16DBT";
			case 863: return "ZHT32TRIS";
			case 864: return "ZHT16DBCS";
			case 865: return "ZHT16BIG5";
			case 866: return "ZHT16CCDC";
			case 867: return "ZHT16MSWIN950";
			case 868: return "ZHT16HKSCS";
			case 870: return "AL24UTFFSS"; // UNICODE_1
			case 871: return "UTF8"; // UNICODE_2
			case 872: return "UTFE";
			case 873: return "AL32UTF8";
			case 992: return "ZHT16HKSCS31";
			case 996: return "KO16TSTSET";
			case 997: return "JA16TSTSET2";
			case 998: return "JA16TSTSET";
			case 1001: return "US16TSTFIXED";
			case 2000: return "AL16UTF16";
			case 2002: return "AL16UTF16LE";
			default: throw new RuntimeException("Unsupported character set");
			}
		}
	}

	public class CreationDate extends Once<Date> {
		public CreationDate(Reader reader) {
			super(() -> {
				byte[] bytes = reader.read(7);
				Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
				calendar.set(
					((bytes[0] & 0xff) << 8) | (bytes[1] & 0xff), // Year...
					bytes[2] - 1, // ... month
					bytes[3], // ... etc.
					bytes[4],
					bytes[5],
					bytes[6]);
				return calendar.getTime();
			});
		}

		public String description() {
			return value().get().toString();
		}
	}

	public class MasterOffset extends Once<Long> {
		public MasterOffset(Reader reader) {
			super(() -> reader.read4());
		}
	}

	public class MasterSize extends Once<Long> {
		public MasterSize(Reader reader) {
			super(() -> reader.read8());
		}
	}

	public class Unknown extends Once<Void> {
		public Unknown(Reader reader, int numberOfBytes) {
			super(() -> {
				reader.skip(numberOfBytes);
				return null;
			});
		}

		public Void get() {
			return value().orElse(null);
		}
	}
}

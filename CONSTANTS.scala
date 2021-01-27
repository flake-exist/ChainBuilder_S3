import java.time._

object CONSTANTS {
  val DATE_PATTERN = "[12]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01])".r // Pattern for RegEx . Check correct string `Date` or not
  val DATE_UNIX_TIME_STAMP = new java.text.SimpleDateFormat("yyyy-MM-dd") // Pattern to convert date(String) into Unix Time Stamp
  val CONVERSION_SYMBOL    : String = "get@conv"
  val NO_CONVERSION_SYMBOL : String = "get@no_conv"
  val GLUE_SYMBOL          : String = "::_" //Use it to concatenate `channel` columns with `conversion` columns
  val GLUE_SYMBOL_POS      : String = GLUE_SYMBOL + CONVERSION_SYMBOL //symbol denotes the contact with channel ended up with conversion
  val GLUE_SYMBOL_NEG      : String = GLUE_SYMBOL + NO_CONVERSION_SYMBOL //symbol denotes the contact with channel ended up without conversion
  val TRANSIT              : String = "=>"
  val IS_NOT_GOAL          : String = "0"
  val CONCAT_SYMBOL        : String = "_>>_"
//  val PROFILEID            : String = "PROFILEID"
//  val SESSIONID            : String = "SESSIONID"
//  val TRANSACTIONID        : String = "TRANSACTIONID"
  val OPTION_PREFIX        : String = "-"
  val ZERO_HTS             : Long   = 0


  val necessary_args = Map(
    "date_start"      -> "String",
    "date_tHOLD"      -> "String",
    "date_finish"     -> "String",
    "target_numbers"  -> "String",
    "product_name"    -> "String",
    "source_platform" -> "String",
    "achieve_mode"    -> "Boolean",
    "source_path"     -> "String",
    "output_path"     -> "String",
    "output_pathD"    -> "String",
    "channel_depth"   -> "String")

  val argumentLineUsage = necessary_args.keys.toList.map( k => OPTION_PREFIX + k + "=value").mkString(" ")

  //Correctly process `options` and `values` from input argument line (Array[String])
  case class OptionMap(opt: String, value: String) {
    // `option` processing
    val getOpt: String = opt.split(OPTION_PREFIX) match {
      case (a@Array(_*)) => a.last
      case as: Array[_] => as(0)
      case _ => throw new Exception(s"Parsing Error on $opt")
    }
    //`value` processing
    val getVal: List[String] = value.split(",").toList.map(_.trim)
  }


  //Parse input arguments from command line.Convert position arguments to named arguments
  def argsPars(args: Array[String], usage: String = argumentLineUsage): collection.mutable.Map[String, List[String]] = {

    if (args.length == 0) {
      throw new Exception(s"Empty argument Array. $usage")
    }

    val interface = collection.mutable.Map[String, List[String]]()
    val (options, _) = args.partition(_.startsWith("-")) // Filter only those strings starting with `splitter` sign

    options.map { elem =>
      val pairUntrust = elem.split("=")
      val pair_trust = pairUntrust match {
        case (p@Array(_, _)) => OptionMap(p(0).trim, p(1).trim)
        case _ => throw new Exception(s"Can not parse $pairUntrust")
      }
      val opt_val = pair_trust.getOpt -> pair_trust.getVal
      interface += opt_val
    }
    interface
  }


  //Check input arguments types
  def argsValid(optionsMap: collection.mutable.Map[String, List[String]]): collection.mutable.Map[String, List[Any]] = {

    val validMap = collection.mutable.Map[String, List[Any]]()

    val stock = necessary_args.keys.map(optionsMap.contains(_)).forall(_ == true) // Check if all necessary `options` exist


    // Cast all `options` to it's types
    stock match {
      case true => optionsMap.keys.toList.map { k =>
        necessary_args(k) match {
          case "Long" => try {
            validMap += k -> optionsMap(k).map(_.toLong)
          } catch {
            case _: Throwable => throw new Exception(s"ERROR $k")
          }
          case "Boolean" => try {
            validMap += k -> optionsMap(k).map(_.toBoolean)
          } catch {
            case _: Throwable => throw new Exception(s"ERROR $k")
          }
          case "String" => try {
            validMap += k -> optionsMap(k).map(_.toString)
          } catch {
            case _: Throwable => throw new Exception(s"ERROR $k")
          }
        }
      }
      case false => throw new Exception(s"$argumentLineUsage Bleat gde argumenti?")
    }
    validMap
  }

  case class DateWork(date_tHOLD : String, date_start : String, date_finish : String) {


    // Date pattern matching . Check if date string corresponds to pattern
    def DateCheck(simmple_date: String): String = {
      val date_correct = DATE_PATTERN.findFirstIn(simmple_date) match {
        case Some(s) => s
        case _ => throw new Exception("Incorrect Date Format.Use YYYY-MM_dd format")
      }
      date_correct
    }

    def localDateToUTC(simmple_date: String, local_format: String = "T00:00:00+03:00[Europe/Moscow]"): Long = {
      val sd = DateCheck(simmple_date)
      val zone_date: String = sd + local_format //local date format with timezone
      val utcZoneId = ZoneId.of("UTC")
      val zonedDateTime = ZonedDateTime.parse(zone_date)
      val utcDateTime = zonedDateTime.withZoneSameInstant(utcZoneId) //UTC date
      val unix_time: Long = utcDateTime.toInstant.toEpochMilli //UNIX time
      unix_time
    }

    val get_tHOLD  = this.DateCheck(date_tHOLD)
    val get_start  = this.DateCheck(date_start)
    val get_finish = this.DateCheck(date_finish)

    val getChronology:List[Long] = List(this.localDateToUTC(get_tHOLD), this.localDateToUTC(get_start), this.localDateToUTC(get_finish))

    val correct_chronology = getChronology match {
      case List(x, y, z) if (x <= y) && (y < z) => true
      case _ => false
    }

  }

  //function check value if it equals null or is empty
  def isEmpty(x:String) = x == "null" || x.isEmpty || x == null


  val channel_creator = (channel_depth       : String,
                          src                 : String,
                          interaction_type    : String,
                          utm_source          : String,
                          utm_medium          : String,
                          utm_campaign        : String,
                          utm_content         : String,
                          utm_term            : String,
                          profileID           : String,
                          ga_sessioncount     : String,
                          creative_id         : String,
                          ad_id               : String) => {

    val utm_builder = List(utm_source,utm_medium,utm_campaign,utm_content,utm_term).mkString(CONCAT_SYMBOL)

    val channel = src match {
      case "adriver" | "dcm" if channel_depth == "profile"  => List(interaction_type,utm_builder,profileID).mkString(CONCAT_SYMBOL)
      case "adriver" | "dcm" if channel_depth == "ad"       => List(interaction_type,utm_builder,profileID,ad_id).mkString(CONCAT_SYMBOL)
      case "adriver" | "dcm" if channel_depth == "creative" => List(interaction_type,utm_builder,profileID,ad_id,creative_id).mkString(CONCAT_SYMBOL)
      case "adriver" | "dcm"                                => utm_builder
      case "ga" | "bq" if channel_depth == "session"        => List(utm_builder,ga_sessioncount).mkString(CONCAT_SYMBOL)
      case "ga" | "bq"                                      => utm_builder
      case _ => throw new Exception("Unknown data source or incorrect channel_depth")
    }

    channel

  }

  val depaturePoint = (arr:Seq[Map[String,Long]],date_start:Long,no_conversion_symbol:String) => {
    val (before,after) = arr.partition(_.values.head < date_start)
    val before_valid = before.reverse.takeWhile(_.keySet.head.endsWith(no_conversion_symbol)).reverse
    val path = before_valid ++ after
    path
  }

  val pathCreator = (arr:Seq[Map[String,Long]],mode:Boolean,conv_symbol:String,no_conv_symbol:String) => {

    val arr1 = arr.foldLeft(List.empty[List[Map[String,Long]]]) {
      case (acc,i) if acc.isEmpty & i.keys.head.endsWith(conv_symbol)    => List() :+ List(i) :+ Nil
      case (acc,i) if acc.isEmpty & i.keys.head.endsWith(no_conv_symbol) => List() :+ List(i)
      case (acc,i) if i.keys.head.endsWith(conv_symbol)                  => acc.init :+ (acc.last :+ i) :+ Nil
      case (acc,i) if i.keys.head.endsWith(no_conv_symbol)               => acc.init :+ (acc.last :+ i)
      case _                                                             => Nil
    }

    val arr2 = arr1.filter(_ != List())

    val arr3 = mode match {
      case true  => arr2.filter(seq => seq.last.keys.head.endsWith(conv_symbol))
      case false => arr2.filter(seq => seq.last.keys.head.endsWith(no_conv_symbol))
    }

    val arr4 = arr3.map(_.dropRight(1))

    arr4

  }

  val chl_hts_Extractor = (arr:Seq[Map[String,Long]],metric:String) => {
    val arr1 = arr match {
      case Nil if metric == "CHL"   => List("singleGoal").mkString(TRANSIT)
      case Nil if metric == "HTS"   => List(0.toString).mkString(TRANSIT)
      case x @ _ if metric == "CHL" => x.map(_.keys.head.replace(GLUE_SYMBOL_NEG,"")).mkString(TRANSIT)
      case y @ _ if metric == "HTS" => y.map(_.values.head.toString).mkString(TRANSIT)
    }
    arr1
  }

}
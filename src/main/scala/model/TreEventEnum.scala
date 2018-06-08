package model

object TreEventEnum extends Enumeration {
  val none, start, lturn, rturn, uturn, deaccel, accel, stop,
  lldw, rldw, // LDW left, LDW right
  fcw1, fcw2, // FCW 1, FCW 2
  overlimit,
  distance = Value
}

object TreEventString extends Enumeration {
  val dtcc, cwdrv, cwprk, bw, bbxwn = Value
}

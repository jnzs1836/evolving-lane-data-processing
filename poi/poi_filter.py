from lib.coordinates import gcj02towgs84
from pymongo import MongoClient

if __name__ == '__main__':
    effective_types = ["010000", "010100", "010101", "010102", "010103", "010104", "010105", "010106", "010107",
                       "010108", "010109", "010110", "010200", "010300", "010400", "010500", "010600", "010700",
                       "010800", "010900", "010901", "011000", "020000", "020100", "020101", "020102", "020103",
                       "020200", "020201", "020202", "020203", "020300", "020301", "020400", "020401", "020402",
                       "020403", "020404", "020405", "020406", "020407", "020500", "020600", "020601", "020602",
                       "020700", "020701", "020702", "020703", "020800", "020900", "020901", "020902", "020903",
                       "020904", "020905", "021000", "021001", "021002", "021003", "021004", "021100", "021200",
                       "021201", "021202", "021300", "021400", "021401", "021500", "021501", "021600", "021700",
                       "021800", "021801", "021802", "021803", "021804", "021900", "022000", "022100", "022200",
                       "022300", "022400", "030000", "030100", "030200", "030201", "030202", "030203", "030300",
                       "030301", "030302", "030303", "030400", "030401", "030500", "030501", "030502", "030503",
                       "030504", "030505", "030506", "030507", "030600", "030700", "030701", "030702", "030800",
                       "030801", "030802", "030803", "030900", "031000", "031001", "031002", "031003", "031004",
                       "031005", "031100", "031101", "031102", "031103", "031104", "031200", "031300", "031301",
                       "031302", "031400", "031500", "031501", "031600", "031601", "031700", "031800", "031900",
                       "031901", "031902", "031903", "031904", "032000", "032100", "032200", "032300", "032400",
                       "032500", "040000", "040100", "040101", "040200", "040201", "050000", "050100", "050101",
                       "050102", "050103", "050104", "050105", "050106", "050107", "050108", "050109", "050110",
                       "050111", "050112", "050113", "050114", "050115", "050116", "050117", "050118", "050119",
                       "050120", "050121", "050200", "050201", "050202", "050203", "050204", "050205", "050206",
                       "050207", "050208", "050209", "050210", "050300", "050301", "050302", "050303", "050304",
                       "050305", "050400", "050500", "050501", "050502", "050600", "050700", "050800", "060000",
                       "060100", "060101", "060102", "060200", "060300", "060301", "060302", "060303", "060304",
                       "060305", "060306", "060400", "060401", "060402", "060403", "060404", "060405", "060406",
                       "060407", "060408", "060409", "060410", "060411", "060412", "060500", "060501", "060502",
                       "060600", "060601", "060602", "060603", "060604", "060605", "060606", "060700", "060701",
                       "060702", "060703", "060704", "060705", "060706", "060800", "060900", "060901", "060902",
                       "060903", "060904", "060905", "060906", "060907", "061000", "061001", "061100", "061101",
                       "061102", "061103", "061200", "061201", "061202", "061203", "061204", "061205", "061206",
                       "061207", "061208", "061209", "061210", "061211", "061212", "061213", "061300", "061301",
                       "061302", "070000", "070100", "070200", "070300", "070301", "070302", "070303", "070304",
                       "070305", "070306", "070400", "070401", "070500", "070600", "070601", "070602", "070603",
                       "070604", "070605", "070606", "070607", "070608", "070609", "070700", "070701", "070702",
                       "070703", "070704", "070705", "070706", "070800", "070900", "071000", "071100", "071200",
                       "071300", "071400", "071500", "071600", "071700", "071800", "071801", "071900", "071901",
                       "071902", "071903", "080000", "080100", "080101", "080102", "080103", "080104", "080105",
                       "080106", "080107", "080108", "080109", "080110", "080111", "080112", "080113", "080114",
                       "080115", "080116", "080117", "080200", "080201", "080202", "080300", "080301", "080302",
                       "080303", "080304", "080305", "080306", "080307", "080308", "080400", "080401", "080402",
                       "080500", "080501", "080502", "080503", "080504", "080600", "080601", "080602", "080603",
                       "090000", "090100", "090101", "090102", "090200", "090201", "090202", "090203", "090204",
                       "090205", "090206", "090207", "090208", "090209", "090210", "090211", "090300", "090400",
                       "090500", "090600", "090601", "090602", "090700", "090701", "090702", "100000", "100100",
                       "100101", "100102", "100103", "100104", "100105", "100200", "100201", "110000", "110100",
                       "110101", "110102", "110103", "110104", "110105", "110200", "110201", "110202", "110203",
                       "110204", "110205", "110206", "110207", "110208", "120000", "120100", "120200", "120201",
                       "120202", "120203", "120300", "120301", "120302", "120303", "120304", "130000", "130100",
                       "130101", "130102", "130103", "130104", "130105", "130106", "130107", "130200", "130201",
                       "130202", "130300", "130400", "130401", "130402", "130403", "130404", "130405", "130406",
                       "130407", "130500", "130501", "130502", "130503", "130504", "130505", "130506", "130600",
                       "130601", "130602", "130603", "130604", "130700", "130701", "130702", "130703", "140000",
                       "140100", "140101", "140102", "140200", "140300", "140400", "140500", "140600", "140700",
                       "140800", "140900", "141000", "141100", "141101", "141102", "141103", "141104", "141105",
                       "141200", "141201", "141202", "141203", "141204", "141205", "141206", "141300", "141400",
                       "141500", "150000", "150100", "150200", "150300", "150301", "150302", "150303", "150400",
                       "150500", "150600", "150700", "150701", "150702", "150800", "150900", "150901", "150902",
                       "150903", "151000", "160000", "160100", "160101", "160102", "160103", "160104", "160105",
                       "160106", "160107", "160108", "160109", "160110", "160111", "160112", "160113", "160114",
                       "160115", "160116", "160117", "160118", "160119", "160120", "160121", "160122", "160123",
                       "160124", "160125", "160126", "160127", "160128", "160129", "160130", "160131", "160132",
                       "160133", "160134", "160135", "160136", "160137", "160138", "160139", "160200", "160300",
                       "160301", "160302", "160303", "160304", "160305", "160306", "160307", "160308", "160309",
                       "160310", "160311", "160312", "160313", "160314", "160315", "160316", "160317", "160318",
                       "160319", "160320", "160321", "160322", "160323", "160324", "160325", "160326", "160327",
                       "160328", "160329", "160330", "160331", "160332", "160333", "160334", "160335", "160336",
                       "160400", "160401", "160402", "160403", "160404", "160405", "160406", "160407", "160408",
                       "160500", "160501", "170000", "170100", "170200", "170201", "170202", "170203", "170204",
                       "170205", "170206", "170207", "170208", "170209", "170300", "170400", "170401", "170402",
                       "170403", "170404", "170405", "170406", "170407", "170408", "180000", "180100", "180101",
                       "180102", "180103", "180200", "180201", "180202", "180203", "180300", "180301", "180302",
                       "180400", "180500", "190000", "190100", "190101", "190102", "190103", "190104", "190105",
                       "190106", "190107", "190108", "190109", "190200", "190201", "190202", "190203", "190204",
                       "190205", "190300", "190301", "190302", "190303", "190304", "190305", "190306", "190307",
                       "190400", "190401", "190402", "190403", "190500", "200000", "200100", "200200", "200300"]

    conn = MongoClient()
    db = conn.hangzhou
    poi_collection = db.poi
    poi_list = poi_collection.find()
    count = 0
    effective_count = 0
    for poi in poi_list:
        count += 1
        if count % 2000 == 0:
            print(str(count) + " is done")
        if poi['typeCode'] in effective_types:
            effective_count+=1
    print(effective_count)
        # poi['loc']['coordinates'] = gcj02towgs84(poi['loc']['coordinates'][1],poi['loc']['coordinates'][0])
        # poi_collection.update({'_id': poi['_id']}, poi, True)
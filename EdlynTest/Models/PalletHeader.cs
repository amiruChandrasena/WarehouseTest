using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class PalletHeader
    {
        public PalletHeader()
        {
            PalletDetails = new List<PalletDetail>();
        }

        public DateTime BestBefore { get; set; }
        public string BinLocation { get; set; }
        public int KeepInStageRoom { get; set; }
        public int PalletNumber { get; set; }
        public int PlanNumber { get; set; }
        public DateTime PrintDate { get; set; }
        public string PrintedAt { get; set; }
        public string Quality { get; set; }
        public DateTime RackedTime { get; set; }
        public string RoomTypeStageTwo { get; set; }
        public string Status { get; set; }
        public string TransferStatus { get; set; }
        public string WarehouseId { get; set; }
        public int ConfirmInd { get; set; }
        public string RoomType { get; set; }
        public string StageOneRackedTime { get; set; }
        public int BatchNo { get; set; }
        public string PickingLabel { get; set; }
        public string Originator { get; set; }
        public List<PalletDetail> PalletDetails { get; set; }
        
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class CatalogItem
    {
        public string CatalogCode { get; set; }
        public string Description { get; set; }
        public int LabApprovalAfterStageOne { get; set; }
        public double MaxWaitingHours { get; set; }
        public string PalletType { get; set; }
        public string RoomType { get; set; }
        public string RoomTypeStageTwo { get; set; }
        public int ShelfLife { get; set; }
        public double StageOneRoomHours { get; set; }
        public double StageTwoRoomHours { get; set; }
        public double TimeToMature { get; set; }
        public string UomPallet { get; set; }

    }
}

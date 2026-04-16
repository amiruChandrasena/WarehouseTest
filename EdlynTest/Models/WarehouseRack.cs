using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class WarehouseRack
    {
        public string WarehouseCode { get; set; }
        public string RoomCode { get; set; }
        public string RackCode { get; set; }
        public int CellCount { get; set; }
        public int ShelvesCount { get; set; }
        public string AssignedCatalogCode { get; set; }
        public string ReservedCatalogCode { get; set; }
        public int RackDepth { get; set; }
        public int NumberOfLevels { get; set; }
        public int CurrentUsedCell { get; set; }
        public int BatchNumber { get; set; }
        public int ZoneNumber { get; set; }
        public DateTime BestBefore { get; set; }
        public double FillingSequence { get; set; }
        public DateTime AssignedTime { get; set; }
        public DateTime LastCycleCountTime { get; set; }
        public int SkipValidation { get; set; }
        public string Status { get; set; }
        public string IsleCode { get; set; }
        public string BayCode { get; set; }
        public string LevelCode { get; set; }
        public string PositionCode { get; set; }
        public int IsPick { get; set; }
        public int ReplenishLevel { get; set; }
        public string RackLocationCode { get; set; }
        public double UnitsLeft { get; set; }
        public string UnitOfMeasure { get; set; }
        public int LicensedPalletNo { get; set; }
        public double PickingSequence { get; set; }

        public int PalletCount { get; set; }
        public WarehouseRackCycle RackCycle { get; set; }
    }
}

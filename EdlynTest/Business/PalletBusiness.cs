using Abstractions.ServiceInterfaces;
using Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace Business
{
    public class PalletBusiness
    {
        private readonly IPalletService _palletService;
        private readonly ICatalogService _catalogService;
        private readonly IJobService _jobService;

        public PalletBusiness(IPalletService palletService, ICatalogService catalogService, IJobService jobService)
        {
            _palletService = palletService;
            _catalogService = catalogService;
            _jobService = jobService;
        }

        public TransactionWrapper GetAllRMPalletsByCriteria(string catalogCode, string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            PalletFilterCriteriaModel filterCriteria = new PalletFilterCriteriaModel();
            filterCriteria.CatalogCode = catalogCode;
            filterCriteria.Status = "W";
            filterCriteria.PalletNo = 0;
            filterCriteria.WarehouseCode = warehouseCode;
            filterCriteria.QualityWh = "XX";

            wrapper = _palletService.GetAllRMPalletsByCriteria(filterCriteria);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            

            List<PalletLabelModel> palletLabels = wrapper.ResultSet[0] as List<PalletLabelModel>;
            if (palletLabels.Count == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("There are no pallets for selected criteria");
            }
            
            if (palletLabels[0].Status == "D")
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallet quantity is 0.");
                return wrapper;
            }

            return wrapper;
            
        }
        
        public TransactionWrapper GetAllRMPalletsByCriteria(string rmType, string catalogCode, string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (rmType == "R")
            {
                PalletFilterCriteriaModel filterCriteria = new PalletFilterCriteriaModel();
                filterCriteria.CatalogCode = catalogCode;
                filterCriteria.Status = "W";
                filterCriteria.PalletNo = 0;
                filterCriteria.WarehouseCode = warehouseCode;
                filterCriteria.QualityWh = "XX";

                wrapper = _palletService.GetAllRMPalletsByCriteria(filterCriteria);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                List<PalletLabelModel> header = wrapper.ResultSet[0] as List<PalletLabelModel>;
                if (header[0].Status == "D")
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Pallet quantity is 0.");
                    return wrapper;
                }

                wrapper = _catalogService.GetCatalogByCatalogCode(catalogCode);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                Catalog catalog = wrapper.ResultSet[0] as Catalog;
                wrapper.ResultSet.Clear();

                if (catalog.NoScanRepl == 1)
                {
                    wrapper = _jobService.GetRMRoomConfigByCatalogCode(catalogCode);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }

                    List<RoomConfigModel> roomConfigList = wrapper.ResultSet[0] as List<RoomConfigModel>;

                    filterCriteria = new PalletFilterCriteriaModel();
                    filterCriteria.CatalogCode = catalogCode;
                    filterCriteria.Status = "W";
                    filterCriteria.PalletNo = 0;
                    filterCriteria.WarehouseCode = warehouseCode;
                    filterCriteria.QualityWh = "XX";
                    filterCriteria.LvBinLocation = roomConfigList[0].WarehouseCode + '.' + roomConfigList[0].RoomCode + '.' + roomConfigList[0].RackCode;

                    wrapper = _palletService.GetAllRMPalletsByCriteria(filterCriteria);
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }
                    header = wrapper.ResultSet[0] as List<PalletLabelModel>;
                }

                if (header.Count == 0)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add($"There are no pallets for selected criteria");
                    return wrapper;
                }

                wrapper.IsSuccess = true;
                wrapper.ResultSet.Add(header);
            }
            else if (rmType == "F")
            {
                PalletFilterCriteriaModel filterCriteria = new PalletFilterCriteriaModel();
                filterCriteria.CatalogCode = catalogCode;
                filterCriteria.Status = "W";
                filterCriteria.PalletNo = 0;
                filterCriteria.WarehouseCode = "";//lot_id//warehouseCode;
                filterCriteria.QualityWh = "XX";

                wrapper = _palletService.GetAllRMPalletsByCriteria(filterCriteria);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                wrapper.IsSuccess = true;
            }

            return wrapper;
        }

        public TransactionWrapper GetAllRMPalletsByCriteria(int palletNo, string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            PalletFilterCriteriaModel filterCriteria = new PalletFilterCriteriaModel();
            filterCriteria.Status = "W";
            filterCriteria.PalletNo = palletNo;
            filterCriteria.QualityWh = "XX";
            filterCriteria.LvBinLocation = binLocation;

            wrapper = _palletService.GetAllRMPalletsByCriteria(filterCriteria);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            return wrapper;
        }
        
        public TransactionWrapper GetAllRMPalletsByCriteria(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            wrapper = _palletService.GetPalletDetail(palletNo);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            return wrapper;
        }
        
        public TransactionWrapper GetPalletHeaderByPalletNo(int palletNo)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            wrapper = _palletService.GetPalletHeaderByPalletNo(palletNo);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            return wrapper;
        }

        public TransactionWrapper GetAllRMPalletsByPickingLabel(string pickingLabel)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            PalletFilterCriteriaModel filterCriteria = new PalletFilterCriteriaModel();
            filterCriteria.Status = "W";
            filterCriteria.QualityWh = "XX";
            filterCriteria.LvPickingLabel = pickingLabel;

            wrapper = _palletService.GetAllRMPalletsByCriteria(filterCriteria);

            return wrapper;
        }

        public TransactionWrapper GetRMScanPallet(string scanPalletNumber, string catalogCode, string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            PalletFilterCriteriaModel filterCriteria = new PalletFilterCriteriaModel();
            if (scanPalletNumber.Trim().Contains("."))
            {
                filterCriteria.PalletNo = 0;
                filterCriteria.LvBinLocation = scanPalletNumber;
            }
            else
            {
                bool isNumeric = Int32.TryParse(scanPalletNumber, out int palletNumber);
                if (!isNumeric || palletNumber < 5000000)
                {

                    wrapper = _palletService.GetRMPalletNumberbyScanPalletLabel(scanPalletNumber);
                    if (wrapper.IsSuccess == false)
                    {
                         return wrapper;
                    }

                    PalletLabelModel palletLabel = wrapper.ResultSet[0] as PalletLabelModel;
                    filterCriteria.PalletNo = palletLabel.PalletNumber;
                    filterCriteria.LvBinLocation = "";
                }
                else
                {
                    filterCriteria.PalletNo = palletNumber;
                    filterCriteria.LvBinLocation = "";
                }
            }
           
            filterCriteria.CatalogCode = catalogCode;
            filterCriteria.Status = "W";
            filterCriteria.WarehouseCode = warehouseCode;
            filterCriteria.QualityWh = "XX";

            wrapper = _palletService.GetAllRMPalletsByCriteria(filterCriteria);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }
            List<PalletLabelModel> header = wrapper.ResultSet[0] as List<PalletLabelModel>;

            if (header.Count == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add($"There are no pallets for selected criteria");
                return wrapper;
            }

            wrapper.IsSuccess = true;

            return wrapper;
        }  
        
        public TransactionWrapper GetRMScanPallet(string scanPalletNumber, string warehouseCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            PalletFilterCriteriaModel filterCriteria = new PalletFilterCriteriaModel();
            if (scanPalletNumber.Trim().Contains("."))
            {
                filterCriteria.PalletNo = 0;
                filterCriteria.LvBinLocation = scanPalletNumber;
            }
            else
            {
                wrapper = _palletService.GetRMPalletNumberbyScanPalletLabel(scanPalletNumber);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }
                PalletLabelModel palletLabel = wrapper.ResultSet[0] as PalletLabelModel;

                if (palletLabel.PalletNumber == 0)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add($"There are no pallets for scan label");
                    return wrapper;
                }

                filterCriteria.PalletNo = palletLabel.PalletNumber;
                filterCriteria.LvBinLocation = "";
            }
           
            filterCriteria.Status = "W";
            filterCriteria.WarehouseCode = warehouseCode;
            filterCriteria.QualityWh = "XX";

            wrapper = _palletService.GetAllRMPalletsByCriteria(filterCriteria);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            List<PalletLabelModel> header = wrapper.ResultSet[0] as List<PalletLabelModel>;

            if (header.Count == 0)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add($"There are no pallets for selected criteria");
                return wrapper;
            }

            wrapper.IsSuccess = true;
            wrapper.ResultSet.Add(header);

            return wrapper;
        }

        public TransactionWrapper CreateRMNewPallet(string originator, StockTransferRMNewPalletModel newPallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            int newPalletNumber = 0;

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    PalletHeader palletHeader = new PalletHeader
                    {
                        PrintedAt = "HH-" + originator,
                        PrintDate = DateTime.Now,
                        PlanNumber = -3,
                        TransferStatus = "P",
                        WarehouseId = newPallet.TransitWarehouse,
                        Status = "W",
                        Quality = "G",
                        BinLocation = "PICKED",
                        PickingLabel = ""
                    };

                    wrapper = _palletService.CreatePalletHeader(palletHeader, ref newPalletNumber);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }
                    else if (newPalletNumber == 0)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("New pallet number failed to be created");
                        return wrapper;
                    }
                    else
                    {
                        newPallet.OpenPalletNo = newPalletNumber;
                    }

                    wrapper = _palletService.SetOpenPalletNumber(newPalletNumber, newPallet.TransferNumber);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    scope.Complete();

                    wrapper.IsSuccess = true;
                    wrapper.ResultSet.Add(newPallet);
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }

            return wrapper;
        }

        public TransactionWrapper CloseRMPallet(string originator, StockTransferRMClosePalletModel closePallet)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            if (closePallet.PalletNo > 0)
            {
                using (TransactionScope scope = new TransactionScope())
                {
                    try
                    {
                        PalletLabelModel palletLabel;
                        wrapper = _palletService.GetRMPalletNumberbyScanPalletLabel(closePallet.TagId);
                        if (wrapper.IsSuccess == false)
                        {
                            palletLabel = new PalletLabelModel();
                        }
                        else
                        {
                            palletLabel = wrapper.ResultSet[0] as PalletLabelModel;
                        }

                        if (palletLabel.PalletNumber > 0)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("Issue : Tag ID already exist in system.");
                            return wrapper;
                        }

                        wrapper = _palletService.CloseTranferOpenPallet(originator, closePallet.TransferNo);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        wrapper = _palletService.UpdatePalletHeaderLabelforClosePallet(closePallet.PalletNo, closePallet.TagId);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        scope.Complete();

                        wrapper.IsSuccess = true;
                    }
                    catch (Exception e)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add(e.Message);
                        return wrapper;
                    }
                }
            }
            else
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add($"There are no pallet for selected pallet number");
                return wrapper;
            }

            return wrapper;
        }
    }
}

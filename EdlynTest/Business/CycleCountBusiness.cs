using System;
using System.Collections.Generic;
using System.Text;
using System.Transactions;
using Abstractions.ServiceInterfaces;
using Models;

namespace Business
{
    public class CycleCountBusiness
    {
        private readonly ICycleCountService _cycleCountService;

        public CycleCountBusiness(ICycleCountService cycleCountService)
        {
            _cycleCountService = cycleCountService;
        }

        public TransactionWrapper EmptyPalletsInRack(string binLocation, string originator, bool isEmpty)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    if (isEmpty)
                    {
                        wrapper = _cycleCountService.UpdateStockCountStatus(binLocation);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        List<CycleCountPallet> singlePallet = new List<CycleCountPallet>();
                        CycleCountPallet pallet = new CycleCountPallet();
                        pallet.BestBefore = "";
                        pallet.CatalogCode = "";
                        pallet.PalletNumber = 0;
                        pallet.PalletUnits = 0;
                        singlePallet.Add(pallet);

                        wrapper = _cycleCountService.InsertStockCount(binLocation, originator, singlePallet);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }

                    wrapper = _cycleCountService.GetPalletNumbers(binLocation);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    List<int> palletNumbers = wrapper.ResultSet[0] as List<int>;
                    wrapper.ResultSet.Clear();
                    for (int i = 0; i < palletNumbers.Count; i++)
                    {
                        PalletLocationLog palletLocationLog = new PalletLocationLog
                        {
                            MovedBy = originator,
                            NewLocation = "SC",
                            PalletNo = palletNumbers[i],
                            SyncTime = "",
                            Timestamp = DateTime.Now,
                            ManifestNo = 0,
                            Remark = "Stock Count"
                        };

                        if (_cycleCountService.InsertPalletLocationLog(palletLocationLog) == false)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("InsertPalletLocationLog : Error");
                            return wrapper;
                        }
                    }

                    wrapper = _cycleCountService.UpdatePalletHeader(binLocation, "SC");

                    scope.Complete();
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

        public TransactionWrapper EmptyRMPalletsInRack(string binLocation, string originator, bool isEmpty)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    if (isEmpty)
                    {
                        wrapper = _cycleCountService.UpdateRMStockCountStatus(binLocation);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        List<CycleCountRMPallet> singlePallet = new List<CycleCountRMPallet>();
                        CycleCountRMPallet pallet = new CycleCountRMPallet();
                        pallet.BestBefore = "";
                        pallet.CatalogCode = "";
                        pallet.PalletNumber = 0;
                        pallet.PalletUnits = 0;
                        pallet.StockQuantity = 0;
                        singlePallet.Add(pallet);

                        wrapper = _cycleCountService.InsertRMStockCount(binLocation, originator, singlePallet);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }

                    wrapper = _cycleCountService.GetPalletNumbers(binLocation);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    List<int> palletNumbers = wrapper.ResultSet[0] as List<int>;
                    wrapper.ResultSet.Clear();
                    for (int i = 0; i < palletNumbers.Count; i++)
                    {
                        PalletLocationLog palletLocationLog = new PalletLocationLog
                        {
                            MovedBy = originator,
                            NewLocation = "SC",
                            PalletNo = palletNumbers[i],
                            SyncTime = "",
                            Timestamp = DateTime.Now,
                            ManifestNo = 0,
                            Remark = "Stock Count"
                        };

                        if (_cycleCountService.InsertPalletLocationLog(palletLocationLog) == false)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("InsertPalletLocationLog : Error");
                            return wrapper;
                        }
                    }

                    wrapper = _cycleCountService.UpdatePalletHeader(binLocation, "SC");

                    scope.Complete();
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

        public TransactionWrapper GetPalletsOnRack(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            string[] locationParts = binLocation.Split('.');
            if (locationParts.Length != 3)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPalletOnRack : Invalid rack location");
                return wrapper;
            }

            string warehouseCode = locationParts[0];
            string roomCode = locationParts[1];
            string rackCode = locationParts[2];

            wrapper = _cycleCountService.GetWarehouseRack(warehouseCode, roomCode, rackCode);

            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            WarehouseRack rack = wrapper.ResultSet[0] as WarehouseRack;
            wrapper.ResultSet.Clear();
            if (rack.IsPick == 1)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPalletOnRack : " + binLocation + " is not a bulk bin.");
                return wrapper;
            }

            wrapper = _cycleCountService.GetPalletsInRack(binLocation);
            return wrapper;
        }

        public TransactionWrapper GetRMPalletsInRack(string binLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            string[] locationParts = binLocation.Split('.');
            if (locationParts.Length != 3)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPalletOnRack : Invalid rack location");
                return wrapper;
            }

            string warehouseCode = locationParts[0];
            string roomCode = locationParts[1];
            string rackCode = locationParts[2];

            wrapper = _cycleCountService.GetWarehouseRack(warehouseCode, roomCode, rackCode);

            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            WarehouseRack rack = wrapper.ResultSet[0] as WarehouseRack;
            wrapper.ResultSet.Clear();
            if (rack.IsPick == 1)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPalletOnRack : " + binLocation + " is not a bulk bin.");
                return wrapper;
            }

            wrapper = _cycleCountService.GetRMPalletsInRack(binLocation);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            if (wrapper.ResultSet.Count > 0)
            {
                List<CycleCountRMPallet> rmPallets = wrapper.ResultSet[0] as List<CycleCountRMPallet>;
                
                for (int i = 0; i < rmPallets.Count; i++)
                {
                    decimal orderConversion = 0;
                    decimal stockConversion = 0;

                    wrapper = _cycleCountService.GetConversion(rmPallets[i].UomOrder, ref orderConversion); // get order conversion
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }
                    rmPallets[i].OrderConversion = orderConversion;

                    wrapper = _cycleCountService.GetConversion(rmPallets[i].Uom, ref stockConversion); // get stock conversion
                    if (wrapper.IsSuccess == false)
                    {
                        return wrapper;
                    }
                    if (stockConversion == 0)
                    {
                        rmPallets[i].StockConversion = 1; // avoid dividing by zero
                    } else
                    {
                        rmPallets[i].StockConversion = stockConversion;
                    }
                }

                wrapper.ResultSet.Clear();
                wrapper.ResultSet.Add(rmPallets);
            }

            return wrapper;
        }

        public TransactionWrapper SavePalletsInRack(string binLocation, string originator, List<CycleCountPallet> pallets)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    wrapper = _cycleCountService.UpdateStockCountStatus(binLocation);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    if (pallets != null)
                    {
                        if (pallets.Count > 0)
                        {
                            wrapper = _cycleCountService.UpdatePalletDetail(pallets, originator);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }

                            wrapper = _cycleCountService.InsertStockCount(binLocation, originator, pallets);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }
                        }
                    }

                    scope.Complete();
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

        public TransactionWrapper SaveRMPalletsInRack(string binLocation, string originator, List<CycleCountRMPallet> pallets)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    wrapper = _cycleCountService.UpdateRMStockCountStatus(binLocation);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    if (pallets != null)
                    {
                        if (pallets.Count > 0)
                        {
                            wrapper = _cycleCountService.UpdateRMPalletDetail(pallets, originator);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }

                            wrapper = _cycleCountService.InsertRMStockCount(binLocation, originator, pallets);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }
                        }
                    }

                    scope.Complete();
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
    }
}

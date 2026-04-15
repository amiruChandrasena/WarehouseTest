using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Transactions;
using Abstractions.ServiceInterfaces;
using Models;

namespace Business
{
    public class PutAwayBusiness
    {
        private readonly IPutAwayService _putAwayService;

        public PutAwayBusiness(IPutAwayService putAwayService)
        {
            _putAwayService = putAwayService;
        }
        /// <summary>
        /// This is the first part of processRack for Raw Materials. It is only part of the logic as it is possible for at least two messages with options to be returned to the user
        /// </summary>
        /// <param name="warehouseCode"></param>
        /// <param name="roomCode"></param>
        /// <param name="rackCode"></param>
        /// <param name="originator"></param>
        /// <param name="putAwayModel"></param>
        /// <returns></returns>
        public TransactionWrapper BeginProcessRackRawMaterials(string warehouseCode, string roomCode, string rackCode, string originator, PutAwayModel putAwayModel)
        {
            if (putAwayModel.OptionalRoomType == null)
            {
                putAwayModel.OptionalRoomType = "";
            }
            if (putAwayModel.RoomType == null)
            {
                putAwayModel.RoomType = "";
            }

            TransactionWrapper wrapper = new TransactionWrapper();

            string binLocation = warehouseCode + "." + roomCode + "." + rackCode;
            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    PalletLocationLog palletLocationLog = new PalletLocationLog
                    {
                        Timestamp = DateTime.Now,
                        PalletNo = 0,
                        NewLocation = "",
                        MovedBy = originator,
                        Remark = "Attempting Rack " + warehouseCode + "." + roomCode + "." + rackCode + " for " + putAwayModel.Pallet.PalletNumber.ToString()
                    };

                    if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                        return wrapper;
                    }

                    wrapper = _putAwayService.GetWarehouseRack(warehouseCode, roomCode, rackCode, false);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                        {
                            wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                        }
                        return wrapper;
                    }

                    WarehouseRack rack = wrapper.ResultSet[0] as WarehouseRack;

                    if (rack.Status == "T")
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("Cannot move to " + warehouseCode + "." + roomCode + "." + rackCode + ", it is a temporary hold rack.");
                        palletLocationLog.Remark = "This is a temporary hold rack " + warehouseCode + "." + roomCode + "." + rackCode;
                        if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                        {
                            wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                        }
                        return wrapper;
                    }

                    wrapper = _putAwayService.GetWarehouseConfig(warehouseCode, roomCode);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        palletLocationLog.Remark = "WH " + warehouseCode + " ROOM " + roomCode + " not a valid room.";
                        if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                        {
                            wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                        }
                        return wrapper;
                    }

                    WarehouseConfig whConfig = wrapper.ResultSet[0] as WarehouseConfig;

                    if (whConfig.SkipValidation == 0 && putAwayModel.IsMixedPallet == false) // room must be validated because not a mixed pallet
                    {
                        if (!String.IsNullOrEmpty(putAwayModel.OptionalRoomType))
                        {
                            if (whConfig.Type.Contains(putAwayModel.OptionalRoomType))
                            {
                                whConfig.Type += putAwayModel.RoomType;
                            }
                        }
                        if (!whConfig.Type.Contains(putAwayModel.RoomType))
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("Room type[" + whConfig.Type + "] and pallet type[" + putAwayModel.RoomType + "] do not match");
                            palletLocationLog.Remark = "Room type[" + whConfig.Type + "] and pallet type[" + putAwayModel.RoomType + "] do not match";
                            if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                            {
                                wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                            }
                            return wrapper;
                        }
                    }

                    if (putAwayModel.IsMixedPallet == false && rack.IsPick == 0)
                    {
                        if (rack.ReservedCatalogCode != "" && rack.ReservedCatalogCode != putAwayModel.CatalogCode)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("Rack has been reserved for " + rack.ReservedCatalogCode + ". Cannot store " + putAwayModel.CatalogCode);
                            palletLocationLog.Remark = "Rack has been reserved for " + rack.ReservedCatalogCode + ". Cannot store " + putAwayModel.CatalogCode;
                            if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                            {
                                wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                            }
                            return wrapper;
                        }

                        // check for possible consolidation 
                        int consolidate = 0;
                        bool catalogCodeExists = false;
                        wrapper = _putAwayService.CheckRMCatalogCodeExists(binLocation, putAwayModel.Pallet.PalletNumber, putAwayModel.CatalogCode, ref catalogCodeExists);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        if (catalogCodeExists && roomCode != "BULK")
                        {
                            wrapper = _putAwayService.CheckIfConsolidateRM(putAwayModel.CatalogCode, ref consolidate);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }

                            // ask user if they wish to consolidate the items
                            if (consolidate == 1)
                            {
                                wrapper.IsSuccess = true;
                                wrapper.ResultSet.Add("consolidate");
                                wrapper.Messages.Add("This location " + binLocation + " already contains this item(" + putAwayModel.CatalogCode.Trim() + "), do you wish to consolidate stock?");
                                return wrapper;
                            }
                        }

                        wrapper = CheckValidationProcessRackRM(binLocation, originator, putAwayModel.CatalogCode);
                        if (wrapper.IsSuccess == false)
                        {
                            return wrapper;
                        }
                        else if (wrapper.IsSuccess == true && wrapper.Messages.Count > 0)
                        {
                            return wrapper;
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

                wrapper = EndProcessRackRawMaterials(putAwayModel, originator, binLocation);
                
                wrapper.IsSuccess = true;
            }

            return wrapper;
        }
        
        /// <summary>
        /// This checks if the rack can skip validation and returns a message for the user if validation is required
        /// </summary>
        /// <param name="binLocation"></param>
        /// <param name="originator"></param>
        /// <param name="catalogCode"></param>
        /// <returns></returns>
        public TransactionWrapper CheckValidationProcessRackRM(string binLocation, string originator, string catalogCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string[] locationParts = binLocation.Split('.');

            wrapper = _putAwayService.GetWarehouseRack(locationParts[0], locationParts[1], locationParts[2], false);
            if (wrapper.IsSuccess == false)
            {
                return wrapper;
            }

            WarehouseRack rack = wrapper.ResultSet[0] as WarehouseRack;

            if (rack.SkipValidation == 1)
            {
                PalletLocationLog palletLocationLog = new PalletLocationLog
                {
                    Timestamp = DateTime.Now,
                    PalletNo = 0,
                    NewLocation = "",
                    MovedBy = originator,
                    Remark = "Rack " + binLocation + " has skip_validation = TRUE"
                };
                if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                    return wrapper;
                }

                wrapper.IsSuccess = true;
                return wrapper;
            }
            else
            {
                wrapper = _putAwayService.GetProductsInLocation(binLocation);
                if (wrapper.IsSuccess == false)
                {
                    return wrapper;
                }

                List<string> catalogCodes = wrapper.ResultSet[0] as List<string>;
                wrapper.ResultSet.Clear();
                if (catalogCodes.Count > 0)
                {
                    string codes = "";
                    for (int i = 0; i < catalogCodes.Count; i++)
                    {
                        if (!String.IsNullOrEmpty(catalogCodes[i]) && catalogCodes[i] != catalogCode)
                        {
                            if (codes == "")
                            {
                                codes = catalogCodes[i];
                            } else
                            {
                                codes = codes + ", " + catalogCodes[i]; 
                            }
                        }
                    }

                    if (codes != "")
                    {
                        wrapper.ResultSet.Add("validation");
                        wrapper.Messages.Add(codes);
                    }
                    wrapper.IsSuccess = true;
                    return wrapper;
                } else
                {
                    wrapper.IsSuccess = true;
                    return wrapper;
                }
            }

        }

        public TransactionWrapper EndProcessRackRawMaterials(PutAwayModel putAwayModel, string originator, string scannedBinLocation)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            string[] locationParts = scannedBinLocation.Split('.');
            string scannedWarehouseCode = locationParts[0];
            string scannedRoomCode = locationParts[1];
            string scannedRackCode = locationParts[2];

            PalletLocationLog palletLocationLog = new PalletLocationLog
            {
                Timestamp = DateTime.Now,
                PalletNo = 0,
                NewLocation = "",
                MovedBy = originator
            };
            if (!String.IsNullOrEmpty(putAwayModel.CurrentBinLocation))
            {
                palletLocationLog.Remark = "Original Bin " + putAwayModel.CurrentBinLocation;
                if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                    return wrapper;
                }
            }

            string originalBinLocation = "";
            if (putAwayModel.CurrentBinLocation.IndexOf('.') > 0 && putAwayModel.CurrentBinLocation.IndexOf('.') < putAwayModel.CurrentBinLocation.Length)
            {
                originalBinLocation = putAwayModel.CurrentBinLocation;
            }

            using (TransactionScope scope = new TransactionScope())
            {
                double units = 0;
                string status = "";
                wrapper = _putAwayService.GetPalletStockQuantityTotal(putAwayModel.Pallet.PalletNumber, ref units);
                if (wrapper.IsSuccess == false)
                {
                    scope.Dispose();
                    return wrapper;
                }

                if (units == 0)
                {
                    status = "D";
                } else
                {
                    status = "W";
                }

                wrapper = _putAwayService.SetBinLocation(scannedWarehouseCode, scannedRoomCode, scannedRackCode, status, putAwayModel.Pallet.PalletNumber);
                if (wrapper.IsSuccess == false)
                {
                    scope.Dispose();
                    palletLocationLog.Remark = "Cannot UPDATE bin_location " + putAwayModel.Pallet.PalletNumber.ToString();
                    if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                    {
                        wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                    }
                    return wrapper;
                }

                string catalogCode = "";
                if (putAwayModel.IsMixedPallet == false)
                {
                    catalogCode = putAwayModel.CatalogCode;
                }
                // set cell count for new rack
                wrapper = _putAwayService.SetUsedCellCount(scannedWarehouseCode, scannedRoomCode, scannedRackCode, catalogCode, putAwayModel.Pallet.BestBefore);
                if (wrapper.IsSuccess == false)
                {
                    scope.Dispose();
                    palletLocationLog.Remark = "Cannot set cell_count " + putAwayModel.Pallet.PalletNumber.ToString() + " " + scannedBinLocation;
                    if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                    {
                        wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                    }
                    return wrapper;
                }

                // set cell count for original rack (if any)
                if (originalBinLocation != "")
                {
                    locationParts = putAwayModel.CurrentBinLocation.Split('.');
                    string originalWarehouseCode = locationParts[0];
                    string originalRoomCode = locationParts[1];
                    string originalRackCode = locationParts[2];

                    int count = 0;

                    wrapper = _putAwayService.GetWarehouseLocationCount(originalWarehouseCode, originalRoomCode, originalRackCode, ref count);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    if (count == 1)
                    {
                        wrapper = _putAwayService.SetUsedCellCount(originalWarehouseCode, originalRoomCode, originalRackCode, "", DateTime.MinValue);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            palletLocationLog.Remark = "Cannot set original cell_count " + putAwayModel.Pallet.PalletNumber.ToString() + " " + originalBinLocation;
                            if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                            {
                                wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                            }

                            return wrapper;
                        }
                        else
                        {
                            palletLocationLog.Remark = "Original rack cell_count adjusted " + putAwayModel.Pallet.PalletNumber.ToString() + " " + originalBinLocation;
                            if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                            {
                                scope.Dispose();
                                wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                                return wrapper;
                            }
                        }
                    }
                }

                scope.Complete();
            }
            // use separate transaction for
            using (TransactionScope scope = new TransactionScope())
            {
                PalletLocationLog finalLog = new PalletLocationLog
                {
                    Timestamp = DateTime.Now,
                    PalletNo = putAwayModel.Pallet.PalletNumber,
                    NewLocation = scannedBinLocation,
                    MovedBy = originator,
                    Remark = "Moved from: " + putAwayModel.CurrentBinLocation
                };

                if (!_putAwayService.InsertPalletMovementLogRM(finalLog))
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                    return wrapper;
                }

                palletLocationLog.Remark = DateTime.Now.ToShortDateString() + ": Pallet " + putAwayModel.Pallet.PalletNumber.ToString() + " stored in " + scannedBinLocation + " successfully";
                if (!_putAwayService.InsertPalletMovementLogRM(palletLocationLog))
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                    return wrapper;
                }

                scope.Complete();
            }
            wrapper.ResultSet.Clear();
            wrapper.IsSuccess = true;
            return wrapper;
        }

        public TransactionWrapper ProcessRackRMConsolidate(PutAwayModel putAwayModel, string scannedBinLocation, string originator)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    // get the pallet number of the pallet in the location with the same catalog code 
                    int scannedRackPalletNo = 0;
                    string scannedRackPickingLabel = "";


                    wrapper = _putAwayService.GetScannedRMRackPalletNoAndPickingLabel(scannedBinLocation, putAwayModel.CatalogCode, ref scannedRackPalletNo, ref scannedRackPickingLabel);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }
                    else if (scannedRackPalletNo == 0)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("No pallet number with " + putAwayModel.CatalogCode + " found at " + scannedBinLocation);
                        return wrapper;
                    }

                    // get the quantities of the pallet being put away
                    float putAwayStockQty = 0;
                    string putAwayPickingLabel = "";

                    wrapper = _putAwayService.GetPalletStockQuantityRM(putAwayModel.Pallet.PalletNumber, putAwayModel.CatalogCode, ref putAwayStockQty, ref putAwayPickingLabel);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    // if pallet being put away has a label, then if the existing pallet doesn't replace it with the put away one
                    if (!String.IsNullOrEmpty(putAwayPickingLabel) && String.IsNullOrEmpty(scannedRackPickingLabel))
                    {
                        wrapper = _putAwayService.UpdatePalletPickingLabel(putAwayPickingLabel, scannedRackPalletNo);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        wrapper = _putAwayService.UpdatePalletPickingLabel("", putAwayModel.Pallet.PalletNumber);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }
                    }

                    // update existing pallet with the stock from pallet being put away
                    int putAwayPalletQty = Convert.ToInt32(putAwayStockQty);

                    wrapper = _putAwayService.ConsolidatePalletDetailStockQty(putAwayStockQty, putAwayPalletQty, scannedRackPalletNo, putAwayModel.CatalogCode);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    wrapper = _putAwayService.ConsolidatePalletDetailPalletQty(scannedRackPalletNo, putAwayModel.CatalogCode);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }
                    // update the status of the pallet being put away
                    wrapper = _putAwayService.UpdatePalletHeaderStatus(putAwayModel.Pallet.PalletNumber, "T");
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }
                    // update stock of pallet being put away to 0, now that it has been consolidated
                    wrapper = _putAwayService.ConsolidatePalletDetailStockQty(0, 0, putAwayModel.Pallet.PalletNumber, putAwayModel.CatalogCode);

                    PalletLocationLog palletLocationLog = new PalletLocationLog
                    {
                        Timestamp = DateTime.Now,
                        PalletNo = 0,
                        NewLocation = "",
                        MovedBy = originator,
                        Remark = DateTime.Now.ToShortDateString() + ": Pallet " + putAwayModel.Pallet.PalletNumber.ToString() + " consolidated to location " + scannedBinLocation + " successfully."
                    };

                    wrapper.IsSuccess = _putAwayService.InsertPalletMovementLogRM(palletLocationLog);
                    if (wrapper.IsSuccess == false)
                    {
                        wrapper.Messages.Add("InsertPalletMovementLogRM: Error thrown");
                    }

                    // consolidate successful 
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

            return wrapper;
        }

        public TransactionWrapper GetPalletNumbersInRack(string binLocation)
        {
            //TransactionWrapper wrapper = _putAwayService.GetPalletNumbersInRack(binLocation);
            //return wrapper;

            TransactionWrapper wrapper = _putAwayService.GetPalletNumbersInRack(binLocation);
            //if (wrapper.IsSuccess)
            //{
            //    List<Object> palletNos = wrapper.ResultSet[0] as List<Object>;
            //    palletNos = palletNos;//.Distinct().ToArray();

            //    //if (palletNos.Length > 0)
            //    //{
            //    //    Lv_log_msg = "You are going to scan whole rack " + binLocation + " of " + palletNos.Length.ToString() + " pallets into a new location. Press OK to continue.";
            //    //}

            //    wrapper.ResultSet[0] = palletNos;
            //}

            return wrapper;
        }

        public TransactionWrapper GetPalletStatus(int[] palletNumbers)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            for (int i = 0; i < palletNumbers.Length; i++)
            {
                PalletStatusModel palletStatus = new PalletStatusModel();

                palletStatus.PalletNo = palletNumbers[i];

                // Check Manifest Loading Status
                // Check if pallet has been picked
                int count = _putAwayService.CheckIfPalletPicked(palletNumbers[i]);
                bool isPicked = false;
                if (count == -1)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Error checking picked status of pallet");
                    return wrapper;
                }
                else if (count > 0)
                {
                    isPicked = true;
                }
                palletStatus.IsPicked = isPicked;

                // check where the pallet is currently 
                string status = _putAwayService.GetPalletStatus(palletNumbers[i]);
                if (String.IsNullOrEmpty(status))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Error finding status of pallet");
                    return wrapper;
                }
                else if (status.Length > 1)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(status);
                    return wrapper;
                }
                else if (status != "E" && status != "P" && status != "W")
                {
                    palletStatus.IsStatus = false;
                }
                else
                {
                    palletStatus.IsStatus = true;
                }

                // check shelf life
                int shelfLife = _putAwayService.GetPalletShelfLife(palletNumbers[i]);
                if (shelfLife == -1)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Error retrieving shelf life for pallet");
                    return wrapper;
                }
                else if (shelfLife == 0)
                {
                    palletStatus.HasShelfLife = true;
                }
                else
                {
                    DateTime bestBefore = _putAwayService.GetPalletBestBefore(palletNumbers[i]);
                    DateTime dateNow = DateTime.Now;

                    if (bestBefore == DateTime.MinValue)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("No best before found for pallet");
                        return wrapper;
                    }
                    else
                    {
                        if (dateNow.AddDays(shelfLife) >= bestBefore)
                        {
                            palletStatus.HasShelfLife = false;
                        }
                        else
                        {
                            palletStatus.HasShelfLife = true;
                        }
                    }
                }

                // get pallet quality
                string quality = _putAwayService.GetPalletQuality(palletNumbers[i]);
                if (String.IsNullOrEmpty(quality))
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("Could not find quality rating for pallet");
                    return wrapper;
                }
                else if (quality.Length > 1)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(quality);
                    return wrapper;
                }
                else
                {
                    palletStatus.Quality = quality;
                }

                wrapper.ResultSet.Add(palletStatus);
            }

            wrapper.IsSuccess = true;
            wrapper.Messages.Add("Successfully retrieved Pallet Status");
            return wrapper;

        }

        public TransactionWrapper ScanMixPallet(PalletMixModel palletMix)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope()) // big transactions going on, woo boy
            {
                try
                {
                    string BinLocToLastLetter = palletMix.BinLocationTo[palletMix.BinLocationTo.Length - 1].ToString();

                    if (palletMix.BinLocationTo.Length == 4 && "ABCDEFGH".Contains(BinLocToLastLetter))
                    {
                        palletMix.BinLocationTo = string.Concat(palletMix.WarehouseCode, ".", palletMix.RackingZone, ".", palletMix.BinLocationTo);
                    }

                    wrapper = _putAwayService.GetWarehouseRackByBinLocation(palletMix.BinLocationTo);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        return wrapper;
                    }

                    int newPalletNumber = 0;
                    WarehouseRack warehouseRack = wrapper.ResultSet[0] as WarehouseRack;

                    //Pallet Header Insert 
                    if (warehouseRack.LicensedPalletNo == 0)
                    {
                        PalletHeader palletHeader = new PalletHeader
                        {
                            PrintedAt = "HH-" + palletMix.Originator,
                            PrintDate = DateTime.Now,
                            PlanNumber = -1,
                            TransferStatus = "P",
                            WarehouseId = palletMix.WarehouseCode,
                            Status = "W",
                            Quality = "G",
                            BinLocation = palletMix.BinLocationTo,
                            PickingLabel = ""
                        };

                        wrapper = _putAwayService.CreatePalletHeaderForMixPallet(palletHeader, ref newPalletNumber);
                        warehouseRack.LicensedPalletNo = newPalletNumber;

                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        if (!_putAwayService.UpdateRoomConfigLicencedPalletNo(warehouseRack))
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        PalletLocationLog palletLocationLog = new PalletLocationLog
                        {
                            Timestamp = DateTime.Now,
                            PalletNo = warehouseRack.LicensedPalletNo,
                            NewLocation = palletMix.BinLocationTo,
                            MovedBy = palletMix.Originator,
                            SyncTime = "",
                            ManifestNo = 0,
                            Remark = "HH Licenced Pallet"
                        };

                        wrapper.IsSuccess = _putAwayService.InsertPalletLocationLog(palletLocationLog);
                        if (wrapper.IsSuccess == false)
                        {
                            wrapper.Messages.Add("Error inserting to location log table."); // this doesn't need to hold up moving the pallet
                        }
                    }

                    //If Pallet No == 0 then stop
                    if (warehouseRack.LicensedPalletNo == 0)
                    {
                        scope.Dispose();
                        wrapper.Messages.Add("Scan a bin !");
                        return wrapper;
                    }

                    //Start Pallet details Process
                    if (palletMix.palletDetail.PalletUnits > 0)
                    {
                        int IsRowEffected = 0;

                        wrapper = _putAwayService.GetPalletDetailForMixPallet(warehouseRack.LicensedPalletNo,
                            palletMix.palletDetail.CatalogCode,
                            palletMix.palletDetail.BestBefore);

                        if (wrapper.ResultSet.Count > 0)
                        {
                            PalletDetail palletDetail = wrapper.ResultSet[0] as PalletDetail;

                            if (palletDetail != null && palletDetail.PalletNumber > 0)
                            {
                                wrapper = _putAwayService.UpdateNewPalletDetailForMixPallet(palletMix.palletDetail,
                                    warehouseRack.LicensedPalletNo,
                                    ref IsRowEffected);
                            }
                        }
                        else if (IsRowEffected == 0)
                        {
                            wrapper = _putAwayService.InsertNewPalletDetailForMixPallet(warehouseRack.LicensedPalletNo,
                                palletMix.palletDetail.PalletNumber, palletMix.palletDetail);
                        }
                        
                        wrapper = _putAwayService.UpdateOldPalletDetailForMixPallet(palletMix.palletDetail,
                                palletMix.palletDetail.PalletNumber,
                                ref IsRowEffected);
                    }

                    wrapper = _putAwayService.DeleteEmptyPalletDetail(warehouseRack.LicensedPalletNo);

                    wrapper = _putAwayService.DeleteEmptyPalletDetail(palletMix.palletDetail.PalletNumber);

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

            return wrapper;
        }

        public TransactionWrapper ProcessRack(string warehouseCode, string roomCode, string rackCode, string originator, List<PutAwayModel> pallets)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope()) // big transactions going on, woo boy
            {
                try
                {
                    foreach (PutAwayModel pallet in pallets)
                    {
                        wrapper = _putAwayService.GetWarehouseRack(warehouseCode, roomCode, rackCode, false);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        WarehouseRack rack = wrapper.ResultSet[0] as WarehouseRack;
                        wrapper.ResultSet.Clear();

                        if (rack.Status == "T") // is a temporary rack
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("Cannot move to " + warehouseCode + "." + roomCode + "." + rackCode + ", it is a temporary hold rack.");
                            return wrapper;
                        }

                        wrapper = _putAwayService.GetWarehouseConfig(warehouseCode, roomCode);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        WarehouseConfig whConfig = wrapper.ResultSet[0] as WarehouseConfig;
                        wrapper.ResultSet.Clear();

                        if (whConfig.SkipValidation == 0 && pallet.IsMixedPallet == false) // room must be validated because not a mixed pallet
                        {
                            if (pallet.OptionalRoomType != "")
                            {
                                if (whConfig.Type.Contains(pallet.OptionalRoomType))
                                {
                                    whConfig.Type += pallet.RoomType;
                                }
                            }
                            if (!whConfig.Type.Contains(pallet.RoomType))
                            {
                                scope.Dispose();
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("Room type[" + whConfig.Type + "] and pallet type[" + pallet.RoomType + "] do not match");
                                return wrapper;
                            }
                        }

                        if (rack.IsPick == 1)
                        {
                            if (pallet.IsMixedPallet == false)
                            {
                                if (rack.ReservedCatalogCode != "" && rack.ReservedCatalogCode != pallet.CatalogCode)
                                {
                                    scope.Dispose();
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add("Rack has been reserved for " + rack.ReservedCatalogCode + ". Cannot store " + pallet.CatalogCode);
                                    return wrapper;
                                }
                            }
                            else
                            {
                                int productCount = _putAwayService.GetProductCountOnPallet(pallet.Pallet.PalletNumber, rack.ReservedCatalogCode);
                                if (productCount == -1)
                                {
                                    scope.Dispose();
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add("GetProductCountOnPallet : Exception Thrown");
                                    return wrapper;
                                }
                                else if (productCount == 0)
                                {
                                    scope.Dispose();
                                    wrapper.IsSuccess = false;
                                    wrapper.Messages.Add("Mixed pallet " + pallet.Pallet.PalletNumber.ToString() + " has no products matching rack " +
                                        "reserved product code " + pallet.CatalogCode);
                                    return wrapper;
                                }
                            }
                        }

                        if (pallet.IsMixedPallet == false && rack.IsPick == 0)
                        {
                            if (rack.ReservedCatalogCode != "" && rack.ReservedCatalogCode != pallet.CatalogCode)
                            {
                                scope.Dispose();
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("Rack has been reserved for " + rack.ReservedCatalogCode + ". Cannot store " + pallet.CatalogCode);
                                return wrapper;
                            }

                            if (rack.SkipValidation == 0) // do some validation
                            {
                                wrapper = _putAwayService.CheckProductsInRack(warehouseCode, roomCode, rackCode, pallet.CatalogCode);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }
                            }
                        }

                        string originalBinLocation = "";
                        if (pallet.CurrentBinLocation.Contains(".") && pallet.CurrentBinLocation.IndexOf('.') < pallet.CurrentBinLocation.Length)
                        {
                            originalBinLocation = pallet.CurrentBinLocation;
                        }

                        if (rack.IsPick == 1)
                        {
                            int licensedPalletNo = 0;
                            string reservedCatalogCode = "";

                            wrapper = _putAwayService.GetRackLicensedPalletAndCatalogCode(ref licensedPalletNo, ref reservedCatalogCode, warehouseCode,
                                                                                            roomCode, rackCode);
                            if (wrapper.IsSuccess == false)
                            {
                                scope.Dispose();
                                return wrapper;
                            }

                            if (licensedPalletNo == 0)
                            {
                                wrapper = _putAwayService.SetLicensedPallet(pallet.Pallet.PalletNumber, pallet.CatalogCode, warehouseCode, roomCode,
                                                                            rackCode);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }
                            }
                            else if (reservedCatalogCode == pallet.CatalogCode)
                            {
                                wrapper = _putAwayService.SetPalletLocation(warehouseCode, roomCode, rackCode, licensedPalletNo);
                                if (warehouseCode != "E2")
                                {
                                    if (wrapper.IsSuccess == false)
                                    {
                                        scope.Dispose();
                                        return wrapper;
                                    }
                                }

                                // We call this method bottom on this if condition 2024/02/29
                                //wrapper = _putAwayService.DeleteEmptyPalletDetail(licensedPalletNo);
                                //if (wrapper.IsSuccess == false)
                                //{
                                //    scope.Dispose();
                                //    return wrapper;
                                //}

                                wrapper = _putAwayService.GetPalletDetailForUpdate(pallet.Pallet.PalletNumber, pallet.CatalogCode);
                                if (wrapper.IsSuccess == false)
                                {
                                    scope.Dispose();
                                    return wrapper;
                                }

                                if (wrapper.ResultSet.Count > 0)
                                {
                                    foreach (object obj in wrapper.ResultSet)
                                    {
                                        PalletDetail palletDetail = obj as PalletDetail;
                                        int rowsAffected = 0;
                                        TransactionWrapper updateWrapper = _putAwayService.UpdatePalletDetail(ref rowsAffected, palletDetail, licensedPalletNo,
                                                                                                             pallet.Pallet.PalletNumber, pallet.CatalogCode);
                                        if (updateWrapper.IsSuccess == false)
                                        {
                                            scope.Dispose();
                                            return updateWrapper;
                                        }
                                        else if (rowsAffected == 0)
                                        {
                                            updateWrapper = _putAwayService.InsertPalletDetail(licensedPalletNo, pallet.Pallet.PalletNumber,
                                                                                                pallet.CatalogCode, palletDetail);
                                            if (updateWrapper.IsSuccess == false)
                                            {
                                                scope.Dispose();
                                                return updateWrapper;
                                            }
                                        }
                                        else if (rowsAffected > 1)
                                        {
                                            scope.Dispose();
                                            updateWrapper.IsSuccess = false;
                                            updateWrapper.Messages.Add("Please count the rack first");
                                            return updateWrapper;
                                        }

                                        updateWrapper = _putAwayService.UpdateOldPalletDetail(palletDetail, pallet.Pallet.PalletNumber, pallet.CatalogCode);
                                        if (updateWrapper.IsSuccess == false)
                                        {
                                            scope.Dispose();
                                            return updateWrapper;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                scope.Dispose();
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("Rack is reserved for " + reservedCatalogCode.ToString() + ". Cannot continue.");
                                return wrapper;
                            }

                            wrapper = _putAwayService.DeleteEmptyPalletDetail(licensedPalletNo);
                        }
                        int units = _putAwayService.GetPalletUnits(pallet.Pallet.PalletNumber);
                        if (units == -1)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("GetPalletUnits : Error");
                            return wrapper;
                        }

                        string status = "";
                        if (units == 0)
                        {
                            status = "D";
                        }
                        else
                        {
                            status = "W";
                        }
                        wrapper = _putAwayService.SetBinLocation(warehouseCode, roomCode, rackCode, status, pallet.Pallet.PalletNumber);

                        string rackCatalogCode = "";
                        if (!pallet.IsMixedPallet)
                        {
                            rackCatalogCode = pallet.CatalogCode;
                        }

                        wrapper = _putAwayService.SetUsedCellCount(warehouseCode, roomCode, rackCode, rackCatalogCode, pallet.Pallet.BestBefore);
                        if (wrapper.IsSuccess == false)
                        {
                            scope.Dispose();
                            return wrapper;
                        }

                        PalletLocationLog palletLocationLog = new PalletLocationLog
                        {
                            Timestamp = DateTime.Now,
                            PalletNo = pallet.Pallet.PalletNumber,
                            NewLocation = warehouseCode + "." + roomCode + "." + rackCode,
                            MovedBy = originator
                        };

                        wrapper.IsSuccess = _putAwayService.InsertPalletLocationLog(palletLocationLog);
                        if (wrapper.IsSuccess == false)
                        {
                            wrapper.Messages.Add("Could not insert Pallet Location Log for pallet " + pallet.Pallet.PalletNumber); // this doesn't need to hold up moving the pallet
                        }

                        wrapper = _putAwayService.DeleteEmptyPalletDetail(pallet.Pallet.PalletNumber);
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

            return wrapper;
        }

        public TransactionWrapper ValidatePallets(int[] palletNumbers, string originator, string warehouseCode, string roomCode,
                                                  bool isReplenish)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            List<PalletHeader> pallets = new List<PalletHeader>();

            try
            {

                // validate each pallet

                // get pallet
                for (int i = 0; i < palletNumbers.Length; i++)
                {
                    PutAwayModel putAwayModel = new PutAwayModel();
                    PalletHeader pallet = new PalletHeader();
                    wrapper = _putAwayService.GetPalletDetail(palletNumbers[i]);
                    if (!wrapper.IsSuccess)
                    {
                        return wrapper;
                    }
                    else
                    {
                        pallet = wrapper.ResultSet[0] as PalletHeader;
                    }
                    wrapper.Messages.Clear();
                    wrapper.ResultSet.Clear();


                    if (pallet.PalletDetails.Count == 0)
                    {
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("No pallet details found for pallet: " + palletNumbers[i].ToString());
                        return wrapper;
                    }

                    // check if mixed pallet
                    bool isMixedPallet = false;
                    int uniqueProducts = 0;

                    if (pallet.PalletDetails.Count > 1)
                    {
                        string catalogCode = pallet.PalletDetails[0].CatalogItem.CatalogCode.Trim();
                        foreach (var palletDetail in pallet.PalletDetails)
                        {
                            if (palletDetail.CatalogItem.CatalogCode.Trim() != catalogCode)
                            {
                                isMixedPallet = true;
                                uniqueProducts += 1;
                            }
                        }
                    }
                    if (isMixedPallet)
                    {
                        putAwayModel.Description = "MIXED PALLET";
                        putAwayModel.CatalogCode = uniqueProducts.ToString() + " Products";
                        putAwayModel.RoomType = "";
                        putAwayModel.CurrentBinLocation = pallet.BinLocation;
                        putAwayModel.IsMixedPallet = isMixedPallet;
                        putAwayModel.Pallet = pallet;

                        wrapper.ResultSet.Add(putAwayModel);
                    }
                    else // validate pallet based on current location and stage
                    {
                        DateTime dateNow = DateTime.Now;
                        DateTime stageOneRackedTime = new DateTime();
                        string roomType = "";
                        string optionalRoomType = "";
                        string currentRoomType = "";
                        bool roomTypeAssigned = false;
                        CatalogItem catalogItem = pallet.PalletDetails[0].CatalogItem; // due to above check, all catalog items are the same product

                        // check if single stage storage
                        if (catalogItem.RoomTypeStageTwo == "" || catalogItem.RoomTypeStageTwo == catalogItem.RoomType)
                        {
                            if (dateNow < pallet.PrintDate.AddHours(catalogItem.MaxWaitingHours))
                            {
                                optionalRoomType = "C";
                            }
                        }
                        else if (catalogItem.RoomTypeStageTwo != "") // it is stage 2 storage
                        {
                            if (dateNow < pallet.PrintDate.AddHours(catalogItem.MaxWaitingHours))
                            {
                                roomType = catalogItem.RoomType; // store in stage 1 rooms
                                optionalRoomType = "C";
                            }
                            else // stage 1 or stage 2
                            {
                                if (catalogItem.LabApprovalAfterStageOne == 1) // hot room products etc
                                {
                                    if (pallet.KeepInStageRoom == 1)
                                    {
                                        roomType = catalogItem.RoomType;
                                    }
                                    else
                                    {
                                        roomType = catalogItem.RoomTypeStageTwo;
                                    }
                                }
                                else
                                {
                                    if (pallet.BinLocation.Contains("."))
                                    {
                                        string[] currentBinLocationParts = pallet.BinLocation.Split('.');
                                        wrapper = _putAwayService.GetCurrentLocationRoomType(currentBinLocationParts[0], currentBinLocationParts[1], ref currentRoomType);
                                        if (wrapper.IsSuccess == false)
                                        {
                                            return wrapper;
                                        }
                                    }
                                    if (currentRoomType != "")
                                    {
                                        if (currentRoomType.Contains(catalogItem.RoomTypeStageTwo))
                                        {
                                            roomType = catalogItem.RoomTypeStageTwo;
                                            roomTypeAssigned = true;
                                        }
                                    }
                                    if (!roomTypeAssigned) // not in stage 2
                                    {
                                        if (currentRoomType != "")
                                        {
                                            if (currentRoomType.Contains(catalogItem.RoomType))
                                            {
                                                try
                                                {
                                                    if (!String.IsNullOrEmpty(pallet.StageOneRackedTime))
                                                    {
                                                        stageOneRackedTime = DateTime.Parse(pallet.StageOneRackedTime);
                                                    }
                                                    else
                                                    {
                                                        stageOneRackedTime = DateTime.MinValue;
                                                    }
                                                }
                                                catch (Exception e)
                                                {
                                                    wrapper.IsSuccess = false;
                                                    wrapper.Messages.Add(e.Message);
                                                    return wrapper;
                                                }
                                                if ((dateNow > pallet.RackedTime.AddHours(catalogItem.StageOneRoomHours) && pallet.RackedTime != DateTime.MinValue) ||
                                                        (dateNow > stageOneRackedTime.AddHours(catalogItem.StageOneRoomHours)
                                                        && stageOneRackedTime != DateTime.MinValue))
                                                {
                                                    roomType = catalogItem.RoomTypeStageTwo;
                                                    roomTypeAssigned = true;
                                                    using (TransactionScope scope = new TransactionScope())
                                                    {
                                                        try
                                                        {
                                                            bool isReleased = _putAwayService.ReleasePalletFromStageOneRoom(pallet.PalletNumber);
                                                            if (!isReleased)
                                                            {
                                                                wrapper.IsSuccess = false;
                                                                wrapper.Messages.Add("Could not release pallet " + pallet.PalletNumber.ToString());
                                                                scope.Dispose();
                                                                return wrapper;
                                                            }

                                                            PalletLocationLog palletLog = new PalletLocationLog
                                                            {
                                                                Timestamp = DateTime.Now,
                                                                PalletNo = pallet.PalletNumber,
                                                                NewLocation = "",
                                                                MovedBy = originator,
                                                                Remark = "RELEASED FROM STAGE1-A",
                                                                ManifestNo = 0
                                                            };

                                                            bool isInserted = _putAwayService.InsertPalletLocationLog(palletLog);
                                                            if (!isInserted)
                                                            {
                                                                wrapper.IsSuccess = false;
                                                                wrapper.Messages.Add("Could not insert pallet location log");
                                                                scope.Dispose();
                                                                return wrapper;
                                                            }

                                                            scope.Complete();
                                                        }
                                                        catch { scope.Dispose(); }
                                                    }
                                                }
                                            }
                                        }
                                        else
                                        {
                                            roomType = catalogItem.RoomType;
                                        }
                                    }
                                }
                            }
                        }
                        putAwayModel.Description = catalogItem.Description;
                        putAwayModel.CatalogCode = catalogItem.CatalogCode;
                        if (!String.IsNullOrEmpty(roomType))
                        {
                            putAwayModel.RoomType = roomType;
                        }
                        else
                        {
                            putAwayModel.RoomType = catalogItem.RoomType;
                        }
                        putAwayModel.CurrentBinLocation = pallet.BinLocation;
                        putAwayModel.IsMixedPallet = isMixedPallet;
                        putAwayModel.OptionalRoomType = optionalRoomType;
                        putAwayModel.Pallet = pallet;

                        // get suggested location to move pallet to
                        TransactionWrapper rackWrapper = new TransactionWrapper();
                        if (isReplenish)
                        {
                            rackWrapper = _putAwayService.GetReplenishLocation(warehouseCode, roomCode, catalogItem.CatalogCode);
                            if (rackWrapper.IsSuccess == false)
                            {
                                return rackWrapper;
                            }
                        }
                        else
                        {
                            rackWrapper = _putAwayService.GetReservedActiveRack(warehouseCode, roomCode, catalogItem.CatalogCode, roomType,
                                                                                pallet.BestBefore);
                            if (rackWrapper.IsSuccess == false)
                            {
                                return rackWrapper;
                            }
                            else if (rackWrapper.ResultSet.Count == 0)
                            {
                                rackWrapper = _putAwayService.GetReservedEmptyRack(warehouseCode, roomCode, catalogItem.CatalogCode, roomType);
                                if (rackWrapper.IsSuccess == false)
                                {
                                    return rackWrapper;
                                }
                                else if (rackWrapper.ResultSet.Count == 0)
                                {
                                    rackWrapper = _putAwayService.GetActiveRackByProductAndBestBefore(warehouseCode, roomCode,
                                                                                        catalogItem.CatalogCode, roomType, pallet.BestBefore);
                                    if (rackWrapper.IsSuccess == false)
                                    {
                                        return rackWrapper;
                                    }
                                    else if (rackWrapper.ResultSet.Count == 0)
                                    {
                                        rackWrapper = _putAwayService.GetAssignedEmptyRack(warehouseCode, roomCode,
                                                                                        catalogItem.CatalogCode, roomType, pallet.BestBefore);
                                        if (rackWrapper.IsSuccess == false)
                                        {
                                            return rackWrapper;
                                        }
                                        else if (rackWrapper.ResultSet.Count == 0)
                                        {
                                            rackWrapper = _putAwayService.GetRackByRoomType(warehouseCode, roomCode, roomType);
                                            if (rackWrapper.IsSuccess == false)
                                            {
                                                return rackWrapper;
                                            }
                                            else if (rackWrapper.ResultSet.Count == 1)
                                            {
                                                WarehouseRack rack = rackWrapper.ResultSet[0] as WarehouseRack;
                                                bool isSuccess = _putAwayService.UpdateRackAssignedCatalogCode(rack, catalogItem.CatalogCode,
                                                                                                                pallet.BestBefore);
                                                if (isSuccess == false)
                                                {
                                                    rackWrapper.IsSuccess = false;
                                                    rackWrapper.Messages.Add("UpdateRackAssignedCatalogCode : Failed");
                                                    return rackWrapper;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (rackWrapper.ResultSet.Count == 0)
                        {
                            putAwayModel.SuggestedRack = "No Suggested Rack Found";
                        }
                        else
                        {
                            WarehouseRack rack = rackWrapper.ResultSet[0] as WarehouseRack;
                            putAwayModel.SuggestedRack = rack.WarehouseCode + "." + rack.RoomCode + "." + rack.RackCode;
                        }

                        wrapper.ResultSet.Add(putAwayModel);
                    }
                }

                return wrapper;

            }
            catch (Exception e)
            {
                wrapper.Messages.Add(e.ToString());
                wrapper.IsSuccess = false;
                return wrapper;
            }

        }

        public TransactionWrapper ValidatePalletRawMaterials(string scanData, string originator, string warehouseCode, string roomCode)
        {
            TransactionWrapper wrapper = new TransactionWrapper();
            PutAwayModel putAwayModel = new PutAwayModel();

            // checks if picking label has been scanned and retrieves corresponding pallet number
            // according to DB a picking label can be a rack 
            int palletNo = _putAwayService.GetPalletByTag(scanData);
            if (palletNo == -1)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("GetPalletByTag: Error thrown");
                return wrapper;
            }
            else if (palletNo > 0)
            {
                scanData = palletNo.ToString();
            }

            // at this point if scanData is not a numeric value then it will always fail trying to find a pallet
            bool isNumeric = int.TryParse(scanData, out palletNo);
            if (!isNumeric)
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add(scanData + " is not a valid pallet number or tag");
                return wrapper;
            }

            // insert movement log
            using (TransactionScope scope = new TransactionScope())
            {
                try
                {
                    PalletLocationLog palletLocationLog = new PalletLocationLog
                    {
                        ManifestNo = 0,
                        MovedBy = originator,
                        NewLocation = "",
                        PalletNo = 0,
                        Remark = "New Pallet " + scanData,
                        Timestamp = DateTime.Now
                    };

                    bool isSuccess = _putAwayService.InsertPalletMovementLogRM(palletLocationLog);
                    if (!isSuccess)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("InsertPalletMovementLogRM(1): Error thrown");
                        return wrapper;
                    }
                    else
                    {
                        scope.Complete();
                    }
                }
                catch (Exception e)
                {
                    scope.Dispose();
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add(e.Message);
                    return wrapper;
                }
            }

            bool isMixedPallet = false;

            // get raw materials pallet
            wrapper = _putAwayService.GetPalletDetailRawMaterials(palletNo, scanData);
            if (wrapper.IsSuccess == false)
            {
                using (TransactionScope scope = new TransactionScope())
                {
                    try
                    {
                        PalletLocationLog palletLocationLog = new PalletLocationLog
                        {
                            ManifestNo = 0,
                            MovedBy = originator,
                            NewLocation = "",
                            PalletNo = 0,
                            Remark = "Invalid Pallet " + scanData,
                            Timestamp = DateTime.Now
                        };

                        bool isSuccess = _putAwayService.InsertPalletMovementLogRM(palletLocationLog);
                        if (!isSuccess)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add("InsertPalletMovementLogRM(2): Error thrown");
                            return wrapper;
                        }
                        else
                        {
                            scope.Complete();
                        }
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

            PalletHeader palletHeader = wrapper.ResultSet[0] as PalletHeader;
            wrapper.ResultSet.Clear();

            ////20230808 Add By Irosh Fernando 2023/08/08 - Pallet quantity must be greater than 0
            //if (palletHeader.PalletDetails[0].PalletQuantity <= 0)
            //{
            //    wrapper.IsSuccess = false;
            //    wrapper.Messages.Add("Product quantity is 0.");
            //    return wrapper;
            //}
            //20230808

            //20230824 Add By Irosh Fernando 2023/08/24 - Pallet quantity must be greater than 0
            if (palletHeader.Status == "D")
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("Pallet quantity is 0.");
                return wrapper;
            }

            if (palletHeader.PalletDetails.Count > 1)
            {
                int uniqueProducts = _putAwayService.GetUniqueProductCount(palletNo);
                if (uniqueProducts == -1)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetUniqueProductCount: Error thrown");
                    return wrapper;
                }
                else if (uniqueProducts == 0)
                {
                    wrapper.IsSuccess = false;
                    wrapper.Messages.Add("GetUniqueProductCount: No products found for pallet " + palletNo.ToString());
                    return wrapper;
                }
                else if (uniqueProducts > 1)
                {
                    isMixedPallet = true;
                    PalletLocationLog palletLocationLog = new PalletLocationLog
                    {
                        Timestamp = DateTime.Now,
                        PalletNo = 0,
                        NewLocation = "",
                        MovedBy = originator,
                        Remark = "Mixed Pallet " + palletNo.ToString()
                    };

                    using (TransactionScope scope = new TransactionScope())
                    {
                        try
                        {
                            bool isSuccess = _putAwayService.InsertPalletMovementLogRM(palletLocationLog);
                            if (!isSuccess)
                            {
                                scope.Dispose();
                                wrapper.IsSuccess = false;
                                wrapper.Messages.Add("InsertPalletMovementLogRM(3): Error thrown");
                                return wrapper;
                            }
                            else
                            {
                                scope.Complete();
                            }
                        }
                        catch (Exception e)
                        {
                            scope.Dispose();
                            wrapper.IsSuccess = false;
                            wrapper.Messages.Add(e.Message);
                            return wrapper;
                        }
                    }

                    putAwayModel.Description = "MIXED PALLET";
                    putAwayModel.CatalogCode = uniqueProducts.ToString() + " Products";
                    putAwayModel.RoomType = "";
                    putAwayModel.CurrentBinLocation = palletHeader.BinLocation;
                    putAwayModel.IsMixedPallet = isMixedPallet;
                    putAwayModel.Pallet = palletHeader;

                    wrapper.ResultSet.Add(putAwayModel);
                    wrapper.IsSuccess = true;
                    return wrapper;
                }
            }

            putAwayModel.Pallet = palletHeader;

            if (!isMixedPallet && palletHeader.PalletDetails[0].CatalogCode != "")
            {
                putAwayModel.RoomType = palletHeader.PalletDetails[0].CatalogItem.RoomType;
                putAwayModel.Description = palletHeader.PalletDetails[0].CatalogItem.Description;
                putAwayModel.CatalogCode = palletHeader.PalletDetails[0].CatalogCode;
                putAwayModel.CurrentBinLocation = palletHeader.BinLocation;

                // time to get a suggested rack
                TransactionWrapper rackWrapper = new TransactionWrapper();
                string roomType = putAwayModel.RoomType;
                CatalogItem catalogItem = palletHeader.PalletDetails[0].CatalogItem;

                rackWrapper = _putAwayService.GetReservedActiveRack(warehouseCode, roomCode, catalogItem.CatalogCode, roomType,
                                                                                palletHeader.BestBefore);
                if (rackWrapper.IsSuccess == false)
                {
                    return rackWrapper;
                }
                else if (rackWrapper.ResultSet.Count == 0)
                {
                    rackWrapper = _putAwayService.GetReservedEmptyRack(warehouseCode, roomCode, catalogItem.CatalogCode, roomType);
                    if (rackWrapper.IsSuccess == false)
                    {
                        return rackWrapper;
                    }
                    else if (rackWrapper.ResultSet.Count == 0)
                    {
                        rackWrapper = _putAwayService.GetActiveRackByProductAndBestBefore(warehouseCode, roomCode,
                                                                            catalogItem.CatalogCode, roomType, palletHeader.BestBefore);
                        if (rackWrapper.IsSuccess == false)
                        {
                            return rackWrapper;
                        }
                        else if (rackWrapper.ResultSet.Count == 0)
                        {
                            rackWrapper = _putAwayService.GetAssignedEmptyRack(warehouseCode, roomCode,
                                                                            catalogItem.CatalogCode, roomType, palletHeader.BestBefore);
                            if (rackWrapper.IsSuccess == false)
                            {
                                return rackWrapper;
                            }
                            else if (rackWrapper.ResultSet.Count == 0)
                            {
                                rackWrapper = _putAwayService.GetRackByRoomType(warehouseCode, roomCode, roomType);
                                if (rackWrapper.IsSuccess == false)
                                {
                                    return rackWrapper;
                                }
                                else if (rackWrapper.ResultSet.Count == 1)
                                {
                                    WarehouseRack rack = rackWrapper.ResultSet[0] as WarehouseRack;
                                    bool isSuccess = _putAwayService.UpdateRackAssignedCatalogCode(rack, catalogItem.CatalogCode,
                                                                                                    palletHeader.BestBefore);
                                    if (isSuccess == false)
                                    {
                                        rackWrapper.IsSuccess = false;
                                        rackWrapper.Messages.Add("UpdateRackAssignedCatalogCode : Failed");
                                        return rackWrapper;
                                    }
                                }
                            }
                        }
                    }
                }

                if (rackWrapper.ResultSet.Count == 0)
                {
                    putAwayModel.SuggestedRack = "No Suggested Rack Found";
                }
                else
                {
                    WarehouseRack rack = rackWrapper.ResultSet[0] as WarehouseRack;
                    putAwayModel.SuggestedRack = rack.WarehouseCode + "." + rack.RoomCode + "." + rack.RackCode;
                }

                wrapper.ResultSet.Add(putAwayModel);
                wrapper.IsSuccess = true;
                return wrapper;
            }
            else
            {
                wrapper.IsSuccess = false;
                wrapper.Messages.Add("No product codes found");
                return wrapper;
            }
        }

        public TransactionWrapper ValidateMixPallet(PalletMixModel palletMix)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope()) // big transactions going on, woo boy
            {
                try
                {
                    wrapper = _putAwayService.ValidateMixPallet(palletMix);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("ValidateMixPallet: Catalog code does not match with the reserved catalog code.");
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

            return wrapper;
        }

        public TransactionWrapper ValidateBulkMixPallet(PalletMixModel palletMix)
        {
            TransactionWrapper wrapper = new TransactionWrapper();

            using (TransactionScope scope = new TransactionScope()) // big transactions going on, woo boy
            {
                try
                {
                    wrapper = _putAwayService.ValidateBulkMixPallet(palletMix);
                    if (wrapper.IsSuccess == false)
                    {
                        scope.Dispose();
                        wrapper.IsSuccess = false;
                        wrapper.Messages.Add("ValidateBulkMixPallet: Catalog code does not match with the reserved catalog code.");
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

            return wrapper;
        }
    }
}


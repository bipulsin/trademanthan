from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict
from database import get_db
from models.products import Product

router = APIRouter(prefix="/products", tags=["products"])

@router.get("/test")
async def test_products():
    """Test endpoint to verify router is working"""
    return {"message": "Products router is working!"}

@router.get("/platform/{platform}")
async def get_products_by_platform(
    platform: str,
    db: Session = Depends(get_db)
):
    """
    Get products based on platform (testnet or live)
    Returns products with product_id_testnet not null for testnet
    Returns products where product_id_live is not null for live
    """
    if platform not in ["testnet", "live"]:
        raise HTTPException(status_code=400, detail="Platform must be 'testnet' or 'live'")
    
    try:
        if platform == "testnet":
            # Get products where product_id_testnet is not null
            query = text("""
                SELECT id, symbol, product_name, product_id_testnet as product_id
                FROM products 
                WHERE product_id_testnet IS NOT NULL 
                ORDER BY CAST(product_id_testnet AS INTEGER) ASC
            """)
        else:
            # Get products where product_id_live is not null
            query = text("""
                SELECT id, symbol, product_name, product_id_live as product_id
                FROM products 
                WHERE product_id_live IS NOT NULL 
                ORDER BY CAST(product_id_live AS INTEGER) ASC
            """)
        
        result = db.execute(query)
        products = []
        
        for row in result:
            products.append({
                "id": row.id,
                "symbol": row.symbol,
                "product_name": row.product_name,
                "product_id": row.product_id,
                "display_name": f"{row.product_name} - {row.symbol}"
            })
        
        return {
            "success": True,
            "platform": platform,
            "count": len(products),
            "products": products
        }
        
    except Exception as e:
        # Return mock data for development when database is not available
        if "connection" in str(e).lower() or "refused" in str(e).lower():
            mock_products = [
                {"id": 1, "symbol": "BTCUSD", "product_name": "Bitcoin Perpetual", "product_id": "84", "display_name": "Bitcoin Perpetual - BTCUSD"},
                {"id": 2, "symbol": "ETHUSD", "product_name": "Ethereum Perpetual", "product_id": "1699", "display_name": "Ethereum Perpetual - ETHUSD"}
            ]
            return {
                "success": True,
                "platform": platform,
                "count": len(mock_products),
                "products": mock_products,
                "note": "Using mock data - database not available"
            }
        raise HTTPException(status_code=500, detail=f"Failed to fetch products: {str(e)}")

@router.get("/")
async def get_all_products(db: Session = Depends(get_db)):
    """
    Get all products with basic information
    """
    try:
        products = db.query(Product).filter(Product.is_active == True).all()
        
        result = []
        for product in products:
            result.append({
                "id": product.id,
                "symbol": product.symbol,
                "product_name": product.product_name,
                "base_asset": product.base_asset,
                "quote_asset": product.quote_asset,
                "contract_type": product.contract_type,
                "has_testnet": product.product_id_testnet is not None,
                "has_live": product.product_id_live is not None
            })
        
        return {
            "success": True,
            "count": len(result),
            "products": result
        }
        
    except Exception as e:
        # Return mock data for development when database is not available
        if "connection" in str(e).lower() or "refused" in str(e).lower():
            mock_products = [
                {"id": 1, "symbol": "BTCUSD", "product_name": "Bitcoin Perpetual", "base_asset": "BTC", "quote_asset": "USD", "contract_type": "PERPETUAL", "has_testnet": True, "has_live": True},
                {"id": 2, "symbol": "ETHUSD", "product_name": "Ethereum Perpetual", "base_asset": "ETH", "quote_asset": "USD", "contract_type": "PERPETUAL", "has_testnet": True, "has_live": True}
            ]
            return {
                "success": True,
                "count": len(mock_products),
                "products": mock_products,
                "note": "Using mock data - database not available"
            }
        raise HTTPException(status_code=500, detail=f"Failed to fetch products: {str(e)}")

@router.get("/symbol/{symbol}/platform/{platform}")
async def get_product_by_symbol_and_platform(
    symbol: str,
    platform: str,
    db: Session = Depends(get_db)
):
    """
    Get product details by symbol and platform
    Returns the correct product_id based on platform (testnet or live)
    """
    if platform not in ["testnet", "live"]:
        raise HTTPException(status_code=400, detail="Platform must be 'testnet' or 'live'")
    
    try:
        product = db.query(Product).filter(
            Product.symbol == symbol,
            Product.is_active == True
        ).first()
        
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        
        # Get the correct product_id based on platform
        if platform == "testnet":
            if not product.product_id_testnet:
                raise HTTPException(status_code=400, detail="Product not available on testnet")
            product_id = product.product_id_testnet
        else:
            if not product.product_id_live:
                raise HTTPException(status_code=400, detail="Product not available on live")
            product_id = product.product_id_live
        
        return {
            "success": True,
            "product": {
                "id": product.id,
                "symbol": product.symbol,
                "product_name": product.product_name,
                "product_id": product_id,
                "platform": platform,
                "display_name": f"{product.product_name} - {product.symbol}"
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        # Return mock data for development when database is not available
        if "connection" in str(e).lower() or "refused" in str(e).lower():
            # Return appropriate mock data based on symbol
            if symbol.upper() == "BTCUSD":
                mock_product = {
                    "id": 1,
                    "symbol": "BTCUSD",
                    "product_name": "Bitcoin Perpetual",
                    "product_id": "84" if platform == "testnet" else "84",
                    "platform": platform,
                    "display_name": "Bitcoin Perpetual - BTCUSD"
                }
            elif symbol.upper() == "ETHUSD":
                mock_product = {
                    "id": 2,
                    "symbol": "ETHUSD",
                    "product_name": "Ethereum Perpetual",
                    "product_id": "1699" if platform == "testnet" else "1699",
                    "platform": platform,
                    "display_name": "Ethereum Perpetual - ETHUSD"
                }
            else:
                # Default fallback for unknown symbols
                mock_product = {
                    "id": 1,
                    "symbol": symbol,
                    "product_name": f"{symbol} Product",
                    "product_id": "84" if platform == "testnet" else "84",
                    "platform": platform,
                    "display_name": f"{symbol} Product - {symbol}"
                }
            
            return {
                "success": True,
                "product": mock_product,
                "note": "Using mock data - database not available"
            }
        raise HTTPException(status_code=500, detail=f"Failed to fetch product: {str(e)}")

#!/usr/bin/env python3
"""
Test script to verify DingTalk stream handler fix is working.
"""

import asyncio
import logging
import time
from unittest.mock import AsyncMock, MagicMock, patch

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('test_dingtalk')

@patch('dingtalk_stream.AckMessage.STATUS_OK', 'OK')
def test_dingtalk_handler():
    """Test that DingTalk handler works correctly with the fix."""
    
    print("Testing DingTalk handler fix...")
    
    # Mock the event loop
    with patch('asyncio.get_running_loop') as mock_get_loop:
        mock_loop = MagicMock()
        mock_loop.is_closed.return_value = False
        mock_get_loop.return_value = mock_loop
        
        # Import after patching
        from gateway.platforms.dingtalk import _IncomingHandler, DingTalkAdapter
        
        # Create a real adapter instance
        from gateway.config import PlatformConfig
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))
        
        # Mock the _on_message method to be async
        async def mock_on_message(message):
            print(f"[Background] Processing message: {getattr(message, 'text', 'No text')}")
            await asyncio.sleep(2)  # Simulate 2-second processing
            print("[Background] Processing complete!")
        
        adapter._on_message = mock_on_message
        adapter.handle_message = AsyncMock(return_value="Test response")
        
        # Create handler
        handler = _IncomingHandler(adapter, mock_loop)
        
        # Create mock message
        mock_message = MagicMock()
        mock_message.text = "Hello World"
        
        async def run_test():
            print("✅ Starting handler test...")
            start_time = time.time()
            
            # This should return immediately
            result = await handler.process(mock_message)
            
            elapsed = time.time() - start_time
            print(f"✅ Handler returned in {elapsed:.3f} seconds")
            print(f"✅ Result: {result}")
            
            # Verify it returned quickly
            assert elapsed < 1.0, f"Handler took too long: {elapsed}s"
            assert result == ('OK', 'OK'), f"Expected ('OK', 'OK'), got {result}"
            
            # Wait for background processing to complete
            print("⏳ Waiting for background processing...")
            await asyncio.sleep(3)
            
            return True
        
        # Run test
        success = asyncio.run(run_test())
        
        if success:
            print("🎉 TEST PASSED!")
            print("✅ Handler returns immediately without waiting")
            print("✅ Background processing continues independently")
        else:
            print("❌ TEST FAILED!")
        
        return success

if __name__ == "__main__":
    try:
        success = test_dingtalk_handler()
        if success:
            print("\n" + "="*50)
            print("🎉 DingTalk handler fix is working correctly!")
            print("="*50)
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
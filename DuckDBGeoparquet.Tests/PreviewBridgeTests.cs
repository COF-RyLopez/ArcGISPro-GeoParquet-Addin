using DuckDBGeoparquet.Services;
using System;
using System.Collections.Generic;
using System.Text.Json;
using Xunit;

namespace DuckDBGeoparquet.Tests
{
    public class PreviewBridgeTests
    {
        private readonly List<string> _posted = new();
        private readonly PreviewBridge _bridge;

        public PreviewBridgeTests()
        {
            _bridge = new PreviewBridge(json => _posted.Add(json));
        }

        [Fact]
        public void Constructor_RequiresPostMessageAction()
        {
            Assert.Throws<ArgumentNullException>(() => new PreviewBridge(null));
        }

        [Fact]
        public void ShowExtent_PostsCamelCaseMessage()
        {
            _bridge.ShowExtent(-119.8, 36.7, -119.7, 36.8);

            string json = Assert.Single(_posted);
            using var doc = JsonDocument.Parse(json);
            Assert.Equal("showExtent", doc.RootElement.GetProperty("type").GetString());
            Assert.Equal(-119.8, doc.RootElement.GetProperty("xmin").GetDouble());
            Assert.Equal(36.8, doc.RootElement.GetProperty("ymax").GetDouble());
        }

        [Fact]
        public void AddGeoJsonLayer_OmitsNullRenderer()
        {
            _bridge.AddGeoJsonLayer("Places (sample)", "{\"type\":\"FeatureCollection\",\"features\":[]}");

            string json = Assert.Single(_posted);
            using var doc = JsonDocument.Parse(json);
            Assert.Equal("addGeoJsonLayer", doc.RootElement.GetProperty("type").GetString());
            Assert.Equal("Places (sample)", doc.RootElement.GetProperty("name").GetString());
            Assert.False(doc.RootElement.TryGetProperty("renderer", out _));
        }

        [Fact]
        public void HandleWebMessage_MapReady_SetsFlagAndRaisesEvent()
        {
            bool raised = false;
            _bridge.MapReady += () => raised = true;

            _bridge.HandleWebMessage("{\"type\":\"mapReady\"}");

            Assert.True(raised);
            Assert.True(_bridge.IsMapReady);
        }

        [Fact]
        public void HandleWebMessage_LayerLoaded_RaisesEventWithPayload()
        {
            string name = null;
            int count = 0;
            _bridge.LayerLoaded += (n, c) => { name = n; count = c; };

            _bridge.HandleWebMessage("{\"type\":\"layerLoaded\",\"name\":\"Buildings\",\"featureCount\":42}");

            Assert.Equal("Buildings", name);
            Assert.Equal(42, count);
        }

        [Fact]
        public void HandleWebMessage_ExtentChanged_DefaultsWkidTo4326()
        {
            int wkid = 0;
            _bridge.ExtentChanged += (_, _, _, _, w) => wkid = w;

            _bridge.HandleWebMessage("{\"type\":\"extentChanged\",\"xmin\":1,\"ymin\":2,\"xmax\":3,\"ymax\":4}");

            Assert.Equal(4326, wkid);
        }

        [Fact]
        public void HandleWebMessage_ExtentChangedMissingCoordinates_IsIgnored()
        {
            bool raised = false;
            _bridge.ExtentChanged += (_, _, _, _, _) => raised = true;

            _bridge.HandleWebMessage("{\"type\":\"extentChanged\",\"xmin\":1}");

            Assert.False(raised);
        }

        [Fact]
        public void HandleWebMessage_PreviewUnavailable_RaisesEventWithError()
        {
            string error = null;
            _bridge.PreviewUnavailable += e => error = e;

            _bridge.HandleWebMessage("{\"type\":\"previewUnavailable\",\"error\":\"CDN blocked\"}");

            Assert.Equal("CDN blocked", error);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("not json at all")]
        [InlineData("{\"noType\":true}")]
        [InlineData("{\"type\":\"somethingUnknown\"}")]
        public void HandleWebMessage_MalformedOrUnknownMessages_DoNotThrow(string message)
        {
            var exception = Record.Exception(() => _bridge.HandleWebMessage(message));
            Assert.Null(exception);
        }
    }
}

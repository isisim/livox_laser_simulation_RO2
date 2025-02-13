#include <rclcpp/rclcpp.hpp>
#include <gazebo_ros/node.hpp>
#include <rclcpp/logging.hpp>

#include <gazebo/physics/Model.hh>
#include <gazebo/physics/MultiRayShape.hh>// Store the latest laser scans into laserMsg
#include <gazebo/physics/PhysicsEngine.hh>
#include <gazebo/physics/World.hh>
#include <gazebo/sensors/RaySensor.hh>
#include <gazebo/transport/Node.hh>
#include "ros2_livox/livox_points_plugin.h"
#include "ros2_livox/csv_reader.hpp"
#include "ros2_livox/livox_ode_multiray_shape.h"

#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/point_cloud2_iterator.hpp>

namespace gazebo
{
    #define TIME_ID 0
    #define YAW_ID 1
    #define PITCH_ID 2

    GZ_REGISTER_SENSOR_PLUGIN(LivoxPointsPlugin)

    LivoxPointsPlugin::LivoxPointsPlugin() {}

    LivoxPointsPlugin::~LivoxPointsPlugin() {}

    void convertDataToRotateInfo(const std::vector<std::vector<double>> &datas, std::vector<RotateInfo> &rotate_info, sdf::ElementPtr &scanElem)
    {
        rotate_info.reserve(datas.size());

        auto horizontalElem = scanElem->GetElement("horizontal");
        auto min_h_angle = horizontalElem->Get<double>("min_angle");
        auto max_h_angle = horizontalElem->Get<double>("max_angle");
        if (min_h_angle>max_h_angle){
            auto i = min_h_angle;
            min_h_angle = max_h_angle;
            max_h_angle = i;
        }
        if (min_h_angle < -M_PI) min_h_angle = -M_PI;
        if (max_h_angle > M_PI) max_h_angle = M_PI;

        auto verticalElem = scanElem->GetElement("vertical");
        auto min_v_angle = verticalElem->Get<double>("min_angle");
        auto max_v_angle = verticalElem->Get<double>("max_angle");
        if (min_v_angle>max_v_angle){
            auto i = min_v_angle;
            min_v_angle = max_v_angle;
            max_v_angle = i;
        }
        if (min_v_angle < -M_PI/2) min_v_angle = -M_PI/2;
        if (max_v_angle > M_PI/2) max_v_angle = M_PI/2;

        RCLCPP_INFO(rclcpp::get_logger("convertDataToRotateInfo"), "Limit values: Vertical[%f, %f] - Horizontal[%f,%f]",
                    min_v_angle,max_v_angle,min_h_angle,max_h_angle);
        double largest_h = -M_PI;
        double smallest_h = M_PI;
        double largest_v = -M_PI/2;
        double smallest_v = M_PI/2;
        std::size_t outside_count = 0;
        for (auto &data : datas)
        {
            if (data.size() == 3){
                if(((max_v_angle >= data[PITCH_ID])  && (data[PITCH_ID] >= min_v_angle)) &&
                   ((max_h_angle >= data[YAW_ID]) && (data[YAW_ID] >= min_h_angle))){
                   rotate_info.emplace_back();
                   rotate_info.back().time = data[TIME_ID];
                    // Z upwards, X towards the front and Y to the left
                   rotate_info.back().z_euler = -data[YAW_ID];       // Yaw to Euler Z rotation
                   rotate_info.back().y_euler = -data[PITCH_ID];     // Pitch to Euler Y rotation

                  if(data[PITCH_ID] > largest_v) largest_v = data[PITCH_ID];
                  if(data[PITCH_ID] < smallest_v)smallest_v= data[PITCH_ID];
                  if(data[YAW_ID]> largest_h) largest_h = data[YAW_ID];
                  if(data[YAW_ID]< smallest_h)smallest_h= data[YAW_ID];
                }
                else{
                    outside_count++;
                }
            } else {
                RCLCPP_ERROR(rclcpp::get_logger("convertDataToRotateInfo"), "data size is not 3!");
            }
        }
        RCLCPP_INFO(rclcpp::get_logger("convertDataToRotateInfo"), "Inside Vertical: [%f,%f]",smallest_v,largest_v);
        RCLCPP_INFO(rclcpp::get_logger("convertDataToRotateInfo"), "Inside Horizontal: [%f,%f]",smallest_h,largest_h);
        RCLCPP_INFO(rclcpp::get_logger("convertDataToRotateInfo"), "Outside count: %ld",outside_count);
    }

    void LivoxPointsPlugin::Load(gazebo::sensors::SensorPtr _parent, sdf::ElementPtr sdf)
    {
        node_ = gazebo_ros::Node::Get(sdf);

        std::vector<std::vector<double>> datas;
        std::string file_name = sdf->Get<std::string>("csv_file_name");
        RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "load csv file name: %s", file_name.c_str());
        if (!CsvReader::ReadCsvFile(file_name, datas))
        {   
            RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "cannot get csv file! %s will return !", file_name.c_str());
            return;
        }
        sdfPtr = sdf;
        auto rayElem = sdfPtr->GetElement("ray");
        auto scanElem = rayElem->GetElement("scan");
        auto rangeElem = rayElem->GetElement("range");


        raySensor = _parent;
        auto sensor_pose = raySensor->Pose();
        auto curr_scan_topic = sdf->Get<std::string>("topic");
        RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "ros topic name: %s", curr_scan_topic.c_str());

        child_name = raySensor->Name();
        parent_name = raySensor->ParentName();
        size_t delimiter_pos = parent_name.find("::");
        parent_name = parent_name.substr(delimiter_pos + 2);

        node = transport::NodePtr(new transport::Node());
        node->Init(raySensor->WorldName());
        // PointCloud2 publisher
        cloud2_pub = node_->create_publisher<sensor_msgs::msg::PointCloud2>(curr_scan_topic, 10);

        scanPub = node->Advertise<msgs::LaserScanStamped>(curr_scan_topic+"laserscan", 50);

        rotate_infos.clear();
        convertDataToRotateInfo(datas, rotate_infos, scanElem);
        RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "scan info size: %ld", rotate_infos.size());
        maxPointSize = rotate_infos.size();

        RayPlugin::Load(_parent, sdfPtr);
        laserMsg.mutable_scan()->set_frame(_parent->ParentName());
        // parentEntity = world->GetEntity(_parent->ParentName());
        parentEntity = this->world->EntityByName(_parent->ParentName());
        //SendRosTf(sensor_pose, raySensor->ParentName(), raySensor->Name());
        auto physics = world->Physics();
        laserCollision = physics->CreateCollision("multiray", _parent->ParentName());
        laserCollision->SetName("ray_sensor_collision");
        laserCollision->SetRelativePose(_parent->Pose());
        laserCollision->SetInitialRelativePose(_parent->Pose());
        rayShape.reset(new gazebo::physics::LivoxOdeMultiRayShape(laserCollision));
        laserCollision->SetShape(rayShape);
        samplesStep = sdfPtr->Get<int>("samples");
        downSample = sdfPtr->Get<int>("downsample");
        if (downSample < 1)
        {
            downSample = 1;
        }
        RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "sample: %ld", samplesStep);
        RCLCPP_INFO(rclcpp::get_logger("LivoxPointsPlugin"), "downsample: %ld", downSample);
        rayShape->RayShapes().reserve(samplesStep / downSample);
        rayShape->Load(sdfPtr);
        rayShape->Init();
        minDist = rangeElem->Get<double>("min");
        maxDist = rangeElem->Get<double>("max");
        auto offset = laserCollision->RelativePose();
        ignition::math::Vector3d start_point, end_point;
        for (int j = 0; j < samplesStep; j += downSample)
        {
            int index = j % maxPointSize;
            auto &rotate_info = rotate_infos[index];
            ignition::math::Quaterniond ray;
            ray.Euler(ignition::math::Vector3d(0.0, rotate_info.y_euler, rotate_info.z_euler));
            auto axis = offset.Rot() * ray * ignition::math::Vector3d(1.0, 0.0, 0.0);
            start_point = minDist * axis + offset.Pos();
            end_point = maxDist * axis + offset.Pos();
            rayShape->AddRay(start_point, end_point);
        }
    }

    void LivoxPointsPlugin::OnNewLaserScans()
{
    // Check if rayShape has been initialized
    if (rayShape)
    {
        std::vector<std::pair<int, RotateInfo>> points_pair;
        // Initialize ray scan point pairs
        InitializeRays(points_pair, rayShape);
        rayShape->Update();

        // Create laser scan message and set the timestamp
        msgs::Set(laserMsg.mutable_time(), world->SimTime());
        msgs::LaserScan *scan = laserMsg.mutable_scan();
        InitializeScan(scan);

        // For publishing PointCloud2 type messages
        sensor_msgs::msg::PointCloud2 pcl_msg;
        sensor_msgs::PointCloud2Modifier modifier(pcl_msg);

        modifier.setPointCloud2Fields(4,
          "x", 1, sensor_msgs::msg::PointField::FLOAT32,
          "y", 1, sensor_msgs::msg::PointField::FLOAT32,
          "z", 1, sensor_msgs::msg::PointField::FLOAT32,
          "intensity", 1, sensor_msgs::msg::PointField::FLOAT32
        );

        // Msg header
        pcl_msg.header = std_msgs::msg::Header();
        pcl_msg.header.stamp = node_->get_clock()->now();
        pcl_msg.header.frame_id = raySensor->Name();
        pcl_msg.width = points_pair.size();
        pcl_msg.height = 1;
        pcl_msg.is_dense = true;

        // Resize pointcloud data
        pcl_msg.point_step = 16;
        pcl_msg.row_step = pcl_msg.point_step * pcl_msg.width;
        pcl_msg.data.resize(pcl_msg.row_step);

        //Iterators for PointCloud msg
        sensor_msgs::PointCloud2Iterator<float> iter_x(pcl_msg, "x");
        sensor_msgs::PointCloud2Iterator<float> iter_y(pcl_msg, "y");
        sensor_msgs::PointCloud2Iterator<float> iter_z(pcl_msg, "z");
        sensor_msgs::PointCloud2Iterator<float> iter_intensity(pcl_msg, "intensity");

        // Iterate over ray scan point pairs
        for (auto &pair : points_pair)
        {
            auto range = rayShape->GetRange(pair.first)+minDist;                // ISISIM added to fix ray length
            auto intensity = rayShape->GetRetro(pair.first);

            // Handle out-of-range data
            if (range >= RangeMax())
            {
                range = 0;
            }
            else if (range <= RangeMin())
            {
                range = 0;
            }

            // Calculate point cloud data
            auto rotate_info = pair.second;
            ignition::math::Quaterniond ray;
            ray.Euler(ignition::math::Vector3d(0.0, rotate_info.y_euler, rotate_info.z_euler));
            auto axis = ray * ignition::math::Vector3d(1.0, 0.0, 0.0);
            auto point = range * axis;

            // Fill cloud2 message
            *iter_x = point.X();
            *iter_y = point.Y();
            *iter_z = point.Z();
            *iter_intensity = intensity;

            ++iter_x;
            ++iter_y;
            ++iter_z;
            ++iter_intensity;
        }

        if (scanPub && scanPub->HasConnections()) scanPub->Publish(laserMsg);

        // Publish cloud2
        cloud2_pub->publish(pcl_msg);
    }
}

    void LivoxPointsPlugin::InitializeRays(std::vector<std::pair<int, RotateInfo>> &points_pair,
                                           boost::shared_ptr<physics::LivoxOdeMultiRayShape> &ray_shape)
    {
        auto &rays = ray_shape->RayShapes();
        ignition::math::Vector3d start_point, end_point;
        ignition::math::Quaterniond ray;
        auto offset = laserCollision->RelativePose();
        int64_t end_index = currStartIndex + samplesStep;
        long unsigned int ray_index = 0;
        auto ray_size = rays.size();
        points_pair.reserve(rays.size());
        for (int k = currStartIndex; k < end_index; k += downSample)
        {
            auto index = k % maxPointSize;
            auto &rotate_info = rotate_infos[index];
             ray.Euler(ignition::math::Vector3d(0.0, rotate_info.y_euler, rotate_info.z_euler));
            auto axis = offset.Rot() * ray * ignition::math::Vector3d(1.0, 0.0, 0.0);
            start_point = minDist * axis + offset.Pos();
            end_point = maxDist * axis + offset.Pos();
            if (ray_index < ray_size)
            {
                rays[ray_index]->SetPoints(start_point, end_point);
                points_pair.emplace_back(ray_index, rotate_info);
            }
            ray_index++;
        }
        currStartIndex += samplesStep;
    }

    void LivoxPointsPlugin::InitializeScan(msgs::LaserScan *&scan)
    {
        // Store the latest laser scans into laserMsg
        msgs::Set(scan->mutable_world_pose(), raySensor->Pose() + parentEntity->WorldPose());
        scan->set_angle_min(AngleMin().Radian());
        scan->set_angle_max(AngleMax().Radian());
        scan->set_angle_step(AngleResolution());
        scan->set_count(RangeCount());

        scan->set_vertical_angle_min(VerticalAngleMin().Radian());
        scan->set_vertical_angle_max(VerticalAngleMax().Radian());
        scan->set_vertical_angle_step(VerticalAngleResolution());
        scan->set_vertical_count(VerticalRangeCount());

        scan->set_range_min(RangeMin());
        scan->set_range_max(RangeMax());

        scan->clear_ranges();
        scan->clear_intensities();

        unsigned int rangeCount = RangeCount();
        unsigned int verticalRangeCount = VerticalRangeCount();

        for (unsigned int j = 0; j < verticalRangeCount; ++j)
        {
            for (unsigned int i = 0; i < rangeCount; ++i)
            {
                scan->add_ranges(0);
                scan->add_intensities(0);
            }
        }
    }

    ignition::math::Angle LivoxPointsPlugin::AngleMin() const
    {
        if (rayShape)
            return rayShape->MinAngle();
        else
            return -1;
    }

    ignition::math::Angle LivoxPointsPlugin::AngleMax() const
    {
        if (rayShape)
        {
            return ignition::math::Angle(rayShape->MaxAngle().Radian());
        }
        else
            return -1;
    }

    double LivoxPointsPlugin::GetRangeMin() const { return RangeMin(); }

    double LivoxPointsPlugin::RangeMin() const
    {
        if (rayShape)
            return rayShape->GetMinRange();
        else
            return -1;
    }

    double LivoxPointsPlugin::GetRangeMax() const { return RangeMax(); }

    double LivoxPointsPlugin::RangeMax() const
    {
        if (rayShape)
            return rayShape->GetMaxRange();
        else
            return -1;
    }

    double LivoxPointsPlugin::GetAngleResolution() const { return AngleResolution(); }

    double LivoxPointsPlugin::AngleResolution() const { return (AngleMax() - AngleMin()).Radian() / (RangeCount() - 1); }

    double LivoxPointsPlugin::GetRangeResolution() const { return RangeResolution(); }

    double LivoxPointsPlugin::RangeResolution() const
    {
        if (rayShape)
            return rayShape->GetResRange();
        else
            return -1;
    }

    int LivoxPointsPlugin::GetRayCount() const { return RayCount(); }

    int LivoxPointsPlugin::RayCount() const
    {
        if (rayShape)
            return rayShape->GetSampleCount();
        else
            return -1;
    }

    int LivoxPointsPlugin::GetRangeCount() const { return RangeCount(); }

    int LivoxPointsPlugin::RangeCount() const
    {
        if (rayShape)
            return rayShape->GetSampleCount() * rayShape->GetScanResolution();
        else
            return -1;
    }

    int LivoxPointsPlugin::GetVerticalRayCount() const { return VerticalRayCount(); }

    int LivoxPointsPlugin::VerticalRayCount() const
    {
        if (rayShape)
            return rayShape->GetVerticalSampleCount();
        else
            return -1;
    }

    int LivoxPointsPlugin::GetVerticalRangeCount() const { return VerticalRangeCount(); }

    int LivoxPointsPlugin::VerticalRangeCount() const
    {
        if (rayShape)
            return rayShape->GetVerticalSampleCount() * rayShape->GetVerticalScanResolution();
        else
            return -1;
    }

    ignition::math::Angle LivoxPointsPlugin::VerticalAngleMin() const
    {
        if (rayShape)
        {
            return ignition::math::Angle(rayShape->VerticalMinAngle().Radian());
        }
        else
            return -1;
    }

    ignition::math::Angle LivoxPointsPlugin::VerticalAngleMax() const
    {
        if (rayShape)
        {
            return ignition::math::Angle(rayShape->VerticalMaxAngle().Radian());
        }
        else
            return -1;
    }

    double LivoxPointsPlugin::GetVerticalAngleResolution() const { return VerticalAngleResolution(); }

    double LivoxPointsPlugin::VerticalAngleResolution() const
    {
        return (VerticalAngleMax() - VerticalAngleMin()).Radian() / (VerticalRangeCount() - 1);
    }


}
